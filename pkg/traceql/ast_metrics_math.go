package traceql

import (
	"fmt"
	"math"

	"github.com/grafana/tempo/pkg/tempopb"
	"github.com/prometheus/prometheus/model/labels"
)

var noLabelsSeriesMapKey = SeriesMapKey{}

// mathExpression implements secondStageElement and evaluates binary arithmetic
// operations (+, -, *, /) between metrics sub-query results. The tree structure
// lives here — RootExpr stays flat.
//
// Leaf nodes (op == OpNone) extract their fragment from the input SeriesSet by
// matching __query_fragment labels, then optionally apply a per-leaf filter
// (e.g. topk inside parentheses).
//
// Binary nodes recursively process children and combine with applyBinaryOp.
type mathExpression struct {
	op     Operator           // OpAdd/OpSub/OpMult/OpDiv for binary; OpNone for leaf
	key    string             // leaf only: __query_fragment key to match
	lhs    *mathExpression    // binary only
	rhs    *mathExpression    // binary only
	filter secondStageElement // leaf only: per-leaf second stage (e.g. topk inside parens)
}

var _ secondStageElement = (*mathExpression)(nil)

func (m *mathExpression) String() string {
	if m.op == OpNone {
		s := m.key
		if m.filter != nil {
			s += m.filter.String()
		}
		return s
	}
	return "(" + m.lhs.String() + ") " + m.op.String() + " (" + m.rhs.String() + ")"
}

func (m *mathExpression) validate() error {
	if m.op == OpNone {
		if m.filter != nil {
			return m.filter.validate()
		}
		return nil
	}
	if !m.op.isArithmetic() {
		return fmt.Errorf("unsupported math operation between queries: %s", m.op)
	}
	if err := m.lhs.validate(); err != nil {
		return err
	}
	return m.rhs.validate()
}

func (m *mathExpression) init(req *tempopb.QueryRangeRequest) {
	if m.op == OpNone {
		if m.filter != nil {
			m.filter.init(req)
		}
		return
	}
	m.lhs.init(req)
	m.rhs.init(req)
}

func (m *mathExpression) separator() string {
	return ""
}

// process evaluates the math expression tree against a combined SeriesSet that
// contains series from all sub-queries, tagged with __query_fragment labels.
func (m *mathExpression) process(input SeriesSet) SeriesSet {
	if m.op == OpNone {
		return m.processLeaf(input)
	}

	// Extract metric names from input BEFORE leaves strip them.
	lhsName := m.lhs.metricName(input)
	rhsName := m.rhs.metricName(input)

	lhs := m.lhs.process(input)
	rhs := m.rhs.process(input)

	result := applyBinaryOp(m.op, lhs, rhs)

	// Build combined __name__ label from children.
	combinedName := ""
	if lhsName != "" && rhsName != "" {
		combinedName = fmt.Sprintf("(%s %s %s)", lhsName, m.op.String(), rhsName)
	}

	// Re-key with combined __name__.
	out := make(SeriesSet, len(result))
	for _, v := range result {
		if combinedName != "" {
			v.Labels = append(Labels{Label{Name: labels.MetricName, Value: NewStaticString(combinedName)}}, v.Labels...)
		}
		out[v.Labels.MapKey()] = v
	}
	return out
}

// metricName extracts the __name__ from the input series that would be
// processed by this node. Must be called before process() strips labels.
func (m *mathExpression) metricName(input SeriesSet) string {
	if m.op == OpNone {
		// Leaf: find __name__ in series matching our fragment key
		for _, v := range input {
			fragmentValue := v.Labels.GetValue(internalLabelQueryFragment)
			if fragmentValue.Type == TypeString && fragmentValue.EncodeToString(false) != m.key {
				continue
			}
			for _, l := range v.Labels {
				if l.Name == labels.MetricName {
					return l.Value.EncodeToString(false)
				}
			}
			break
		}
		return ""
	}
	// Binary: build combined name from children
	lhs := m.lhs.metricName(input)
	rhs := m.rhs.metricName(input)
	if lhs == "" || rhs == "" {
		return ""
	}
	return fmt.Sprintf("(%s %s %s)", lhs, m.op.String(), rhs)
}

// processLeaf extracts series matching this leaf's __query_fragment key,
// strips internal labels, and optionally applies a per-leaf filter.
func (m *mathExpression) processLeaf(input SeriesSet) SeriesSet {
	result := make(SeriesSet, len(input))
	for smk, v := range input {
		// Match by __query_fragment
		fragmentValue := v.Labels.GetValue(internalLabelQueryFragment)
		if fragmentValue.Type != TypeString || fragmentValue.EncodeToString(false) != m.key {
			continue
		}

		// Build new key without __query_fragment and __name__
		key := SeriesMapKey{}
		j := 0
		for i := range smk {
			if smk[i].Name == labels.MetricName || smk[i].Name == internalLabelQueryFragment {
				continue
			}
			key[j] = smk[i]
			j++
		}

		// Strip __query_fragment and __name__ from labels
		n := 0
		for _, l := range v.Labels {
			if l.Name != labels.MetricName && l.Name != internalLabelQueryFragment {
				v.Labels[n] = l
				n++
			}
		}
		v.Labels = v.Labels[:n]

		result[key] = v
	}

	if m.filter != nil {
		result = m.filter.process(result)
	}
	return result
}

func applyBinaryOp(op Operator, lhs, rhs SeriesSet) SeriesSet {
	target := lhs
	if _, ok := rhs[noLabelsSeriesMapKey]; !ok {
		target = rhs
	}

	result := make(SeriesSet, len(target))

	// pre-allocate array once to avoid multiple smaller allocations
	var valuesLen int
	for _, t := range target {
		valuesLen += len(t.Values)
	}
	buf := make([]float64, valuesLen)
	var offset int

	for k := range target {
		l, lOk := getTSMatch(lhs, k)
		r, rOk := getTSMatch(rhs, k)
		if !lOk || !rOk {
			continue
		}

		n := min(len(r.Values), len(l.Values))
		values := buf[offset : offset+n]
		for j := 0; j < n; j++ {
			values[j] = applyArithmeticOp(op, l.Values[j], r.Values[j])
		}
		ll := l.Labels
		if len(ll) == 0 {
			ll = r.Labels
		}

		result[k] = TimeSeries{
			Labels:    ll,
			Values:    values,
			Exemplars: mergeExemplars(l.Exemplars, r.Exemplars),
		}
		offset += n
	}
	return result
}

func getTSMatch(set SeriesSet, key SeriesMapKey) (TimeSeries, bool) {
	if s, ok := set[key]; ok {
		return s, true
	}
	if s, ok := set[noLabelsSeriesMapKey]; ok {
		return s, true
	}
	return TimeSeries{}, false
}

func mergeExemplars(a, b []Exemplar) []Exemplar {
	if len(a) == 0 {
		return b
	}
	if len(b) == 0 {
		return a
	}
	result := make([]Exemplar, 0, len(a)+len(b))
	result = append(result, a...)
	result = append(result, b...)
	return result
}

func applyArithmeticOp(op Operator, lhs, rhs float64) float64 {
	if math.IsNaN(lhs) {
		lhs = 0
	}
	if math.IsNaN(rhs) {
		rhs = 0
	}
	switch op {
	case OpAdd:
		return lhs + rhs
	case OpSub:
		return lhs - rhs
	case OpMult:
		return lhs * rhs
	case OpDiv:
		if rhs == 0 {
			return math.NaN()
		}
		return lhs / rhs
	default:
		return math.NaN()
	}
}


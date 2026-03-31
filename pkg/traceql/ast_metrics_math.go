package traceql

import (
	"fmt"
	"math"

	"github.com/grafana/tempo/pkg/tempopb"
	"github.com/prometheus/prometheus/model/labels"
)

var _ secondStageElement = (*Expr)(nil)

// initSecondStage initializes the Expr tree for second stage processing.
// Named differently from firstStageElement.init to avoid conflict.
func (e *Expr) init(req *tempopb.QueryRangeRequest) {
	if e.SecondStage != nil {
		e.SecondStage.init(req)
	}
	if !e.IsLeaf() {
		e.LHS.init(req)
		e.RHS.init(req)
	}
}

func (e *Expr) separator() string { return "" }

// process evaluates this Expr node as a second stage element.
// For leaf: extracts matching fragment, strips internal labels, applies per-leaf filter.
// For binary: recursively processes children, applies arithmetic, rebuilds __name__.
func (e *Expr) process(input SeriesSet) SeriesSet {
	var result SeriesSet
	if e.IsLeaf() {
		result = e.processLeaf(input)
	} else {
		// Extract metric names before children strip them
		lhsName := e.LHS.metricName(input)
		rhsName := e.RHS.metricName(input)

		lhs := e.LHS.process(input)
		rhs := e.RHS.process(input)
		result = applyBinaryOp(e.Op, lhs, rhs)

		// Build combined __name__
		combinedName := ""
		if lhsName != "" && rhsName != "" {
			combinedName = fmt.Sprintf("(%s %s %s)", lhsName, e.Op.String(), rhsName)
		}

		// Re-key with combined __name__
		out := make(SeriesSet, len(result))
		for _, v := range result {
			if combinedName != "" {
				v.Labels = append(Labels{Label{Name: labels.MetricName, Value: NewStaticString(combinedName)}}, v.Labels...)
			}
			out[v.Labels.MapKey()] = v
		}
		result = out
	}

	if e.SecondStage != nil {
		result = e.SecondStage.process(result)
	}
	return result
}

// processLeaf extracts series matching this leaf's fragment key, strips
// __query_fragment and __name__ labels. When series have no __query_fragment
// label (single non-math query), all series are accepted and only
// __query_fragment is stripped (preserving __name__).
func (e *Expr) processLeaf(input SeriesSet) SeriesSet {
	fragmentKey := e.String()

	// Detect if any series has __query_fragment (math query with routing)
	hasFragment := false
	for _, v := range input {
		if fv := v.Labels.GetValue(internalLabelQueryFragment); fv.Type == TypeString {
			hasFragment = true
			break
		}
	}

	result := make(SeriesSet, len(input))
	for smk, v := range input {
		if hasFragment {
			// Math query: match by __query_fragment
			fv := v.Labels.GetValue(internalLabelQueryFragment)
			if fv.Type == TypeString && fv.EncodeToString(false) != fragmentKey {
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
		} else {
			// Single query: no routing, pass through as-is
			result[smk] = v
		}
	}
	return result
}

// metricName extracts __name__ from series matching this node's fragment.
// Must be called before process() strips labels.
func (e *Expr) metricName(input SeriesSet) string {
	if e.IsLeaf() {
		fragmentKey := e.String()
		for _, v := range input {
			fv := v.Labels.GetValue(internalLabelQueryFragment)
			if fv.Type == TypeString && fv.EncodeToString(false) != fragmentKey {
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
	lhs := e.LHS.metricName(input)
	rhs := e.RHS.metricName(input)
	if lhs == "" || rhs == "" {
		return ""
	}
	return fmt.Sprintf("(%s %s %s)", lhs, e.Op.String(), rhs)
}

var noLabelsSeriesMapKey = SeriesMapKey{}

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

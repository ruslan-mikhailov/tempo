package traceql

// AttributeMapper maps a custom attribute's Name (the part after the scope,
// e.g. "http.path" for span.http.path) to a replacement. It is invoked only for
// custom attributes, i.e. Intrinsic == IntrinsicNone with a non-empty Name.
// Intrinsics, scopes (span./resource./parent./...) and structural attributes are
// never passed to it.
type AttributeMapper func(name string) string

// ValueMapper maps a string literal value to a replacement. isRegex is true when
// the value is an operand of a regex comparison (=~ / !~).
type ValueMapper func(value string, isRegex bool) string

// Anonymize parses q and returns an equivalent query in which every custom
// attribute name is replaced via mapAttr and every string literal via mapValue.
// Numbers, durations, booleans, enums (status/kind), intrinsics, scopes and
// hints are left untouched.
//
// The query is parsed without optimizations so that its structure (e.g. OR
// chains) is preserved. An error is returned if q does not parse.
//
// Note: the result is produced via the AST stringer, which normalizes spacing
// and quotes string literals with backticks; only the structure is preserved,
// not the original formatting.
func Anonymize(q string, mapAttr AttributeMapper, mapValue ValueMapper) (string, error) {
	expr, err := ParseNoOptimizations(q)
	if err != nil {
		return "", err
	}

	a := &anonymizer{mapAttr: mapAttr, mapValue: mapValue}
	return a.rewriteRoot(expr).String(), nil
}

type anonymizer struct {
	mapAttr  AttributeMapper
	mapValue ValueMapper
}

// rewriteRoot mirrors fieldExpressionRewriter.RewriteRoot: it rewrites every
// pipeline and its associated metrics processor, recomputes the sub-query map
// keys, and remaps the expression tree's leaf keys so RootExpr.String() stays
// consistent.
func (a *anonymizer) rewriteRoot(r *RootExpr) *RootExpr {
	newPipelines := make(map[string]Pipeline, len(r.Pipeline))
	keyMap := make(map[string]string, len(r.Pipeline)) // old key -> new key

	for k, p := range r.Pipeline {
		newP := a.rewritePipeline(p)
		newKey := newP.String()
		if proc, ok := r.BatchSpanProcessor[k]; ok {
			// proc is shared with SeriesProcessor[k]; mutating it in place updates both.
			a.rewriteMetricsFirstStage(proc)
			newKey += " | " + proc.(Element).String()
		}
		newPipelines[newKey] = newP
		keyMap[k] = newKey
	}

	var newSpanProcs map[string]spanProcessor
	if r.BatchSpanProcessor != nil {
		newSpanProcs = make(map[string]spanProcessor, len(r.BatchSpanProcessor))
		for k, v := range r.BatchSpanProcessor {
			newSpanProcs[keyMap[k]] = v
		}
	}

	var newSeriesProcs map[string]seriesProcessor
	if r.SeriesProcessor != nil {
		newSeriesProcs = make(map[string]seriesProcessor, len(r.SeriesProcessor))
		for k, v := range r.SeriesProcessor {
			newSeriesProcs[keyMap[k]] = v
		}
	}

	return &RootExpr{
		Pipeline:           newPipelines,
		BatchSpanProcessor: newSpanProcs,
		SeriesProcessor:    newSeriesProcs,
		expression:         r.expression.rewriteKeys(keyMap),
		Hints:              r.Hints,
		OptimizationCount:  r.OptimizationCount,
	}
}

func (a *anonymizer) rewritePipeline(p Pipeline) Pipeline {
	elements := make([]PipelineElement, len(p.Elements))
	for i, elem := range p.Elements {
		switch e := elem.(type) {
		case *SpansetFilter:
			e.Expression = a.rewriteFieldExpression(e.Expression, false)
			elements[i] = e
		case SpansetOperation:
			elements[i] = newSpansetOperation(e.Op,
				a.rewriteSpansetExpression(e.LHS),
				a.rewriteSpansetExpression(e.RHS))
		case ScalarFilter:
			elements[i] = newScalarFilter(e.Op,
				a.rewriteScalarExpression(e.LHS),
				a.rewriteScalarExpression(e.RHS))
		case GroupOperation:
			elements[i] = newGroupOperation(a.rewriteFieldExpression(e.Expression, false))
		case SelectOperation:
			elements[i] = SelectOperation{attrs: a.rewriteAttributes(e.attrs)}
		case Aggregate:
			elements[i] = a.rewriteAggregate(e)
		case Pipeline:
			elements[i] = a.rewritePipeline(e)
		default:
			// CoalesceOperation and any other element types carry no attributes
			// or string values, so they pass through unchanged.
			elements[i] = elem
		}
	}
	return newPipeline(elements...)
}

func (a *anonymizer) rewriteSpansetExpression(se SpansetExpression) SpansetExpression {
	switch e := se.(type) {
	case *SpansetFilter:
		e.Expression = a.rewriteFieldExpression(e.Expression, false)
		return e
	case SpansetOperation:
		return newSpansetOperation(e.Op,
			a.rewriteSpansetExpression(e.LHS),
			a.rewriteSpansetExpression(e.RHS))
	case ScalarFilter:
		return newScalarFilter(e.Op,
			a.rewriteScalarExpression(e.LHS),
			a.rewriteScalarExpression(e.RHS))
	case Pipeline:
		return a.rewritePipeline(e)
	default:
		return se
	}
}

func (a *anonymizer) rewriteScalarExpression(se ScalarExpression) ScalarExpression {
	switch e := se.(type) {
	case ScalarOperation:
		return newScalarOperation(e.Op,
			a.rewriteScalarExpression(e.LHS),
			a.rewriteScalarExpression(e.RHS))
	case Aggregate:
		return a.rewriteAggregate(e)
	case Pipeline:
		return a.rewritePipeline(e)
	case Static:
		return a.rewriteStatic(e, false)
	default:
		return se
	}
}

func (a *anonymizer) rewriteAggregate(agg Aggregate) Aggregate {
	if agg.e == nil {
		return agg
	}
	return Aggregate{op: agg.op, e: a.rewriteFieldExpression(agg.e, false)}
}

// rewriteFieldExpression rewrites a field expression. regexCtx is true when the
// expression is (transitively) an operand of a regex comparison, so string
// literals are routed to the regex value mapper.
func (a *anonymizer) rewriteFieldExpression(fe FieldExpression, regexCtx bool) FieldExpression {
	switch e := fe.(type) {
	case *BinaryOperation:
		childRegex := regexCtx || e.Op == OpRegex || e.Op == OpNotRegex
		e.LHS = a.rewriteFieldExpression(e.LHS, childRegex)
		e.RHS = a.rewriteFieldExpression(e.RHS, childRegex)
		return e
	case UnaryOperation:
		return UnaryOperation{Op: e.Op, Expression: a.rewriteFieldExpression(e.Expression, regexCtx)}
	case Static:
		return a.rewriteStatic(e, regexCtx)
	case Attribute:
		return a.rewriteAttribute(e)
	default:
		return fe
	}
}

func (a *anonymizer) rewriteStatic(s Static, regexCtx bool) Static {
	if s.Type != TypeString {
		return s
	}
	return NewStaticString(a.mapValue(s.EncodeToString(false), regexCtx))
}

func (a *anonymizer) rewriteAttribute(attr Attribute) Attribute {
	// Only custom attributes are anonymized; intrinsics and structural
	// attributes carry no user data and must stay intact.
	if attr.Intrinsic != IntrinsicNone || attr.Name == "" {
		return attr
	}
	// attr is a value copy, so mutating Name here does not affect the caller.
	attr.Name = a.mapAttr(attr.Name)
	return attr
}

func (a *anonymizer) rewriteAttributes(attrs []Attribute) []Attribute {
	out := make([]Attribute, len(attrs))
	for i, attr := range attrs {
		out[i] = a.rewriteAttribute(attr)
	}
	return out
}

// rewriteMetricsFirstStage mutates a metrics first-stage processor in place.
// Second-stage processors (topk/bottomk/filters) carry no attributes or string
// values, so they are left untouched.
func (a *anonymizer) rewriteMetricsFirstStage(proc spanProcessor) {
	switch m := proc.(type) {
	case *MetricsAggregate:
		m.attr = a.rewriteAttribute(m.attr)
		for i := range m.by {
			m.by[i] = a.rewriteAttribute(m.by[i])
		}
	case *MetricsCompare:
		if m.f != nil {
			m.f.Expression = a.rewriteFieldExpression(m.f.Expression, false)
		}
	}
}

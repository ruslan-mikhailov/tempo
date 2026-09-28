package traceql

func (r RootExpr) extractConditions(request FetchSpansRequest) map[string]FetchSpansRequest {
	out := make(map[string]FetchSpansRequest, len(r.Pipeline))
	for key, p := range r.Pipeline {
		req := request
		p.extractConditions(&req)
		if sp := r.BatchSpanProcessor[key]; sp != nil {
			sp.extractConditions(&req)
		}
		out[key] = req
	}
	return out
}

func (f SpansetFilter) extractConditions(request *FetchSpansRequest) {
	f.Expression.extractConditions(request)

	// For empty spansets { } ensure there is something that matches all spans.
	// Use start time which would have been selected as part of the second pass
	// metadata, and is still fairly efficient to pull back.
	if s, ok := f.Expression.(Static); ok {
		if b, ok := s.Bool(); !ok || !b {
			return
		}

		for _, c := range request.Conditions {
			if c.Attribute.Intrinsic != IntrinsicNone && c.Op == OpNone {
				// A different match-all intrinsic is already present.
				return
			}
		}

		request.appendCondition(Condition{
			Attribute: NewIntrinsic(IntrinsicSpanStartTime),
			Op:        OpNone,
		})
	}
}

// extractConditions on Select puts its conditions into the SecondPassConditions
func (o SelectOperation) extractConditions(request *FetchSpansRequest) {
	selectR := &FetchSpansRequest{}
	for _, expr := range o.attrs {
		expr.extractConditions(selectR)
	}
	// copy any conditions to the normal request's SecondPassConditions
	request.SecondPassConditions = append(request.SecondPassConditions, selectR.Conditions...)
}

func (o *BinaryOperation) extractConditions(request *FetchSpansRequest) {
	// A positive substring match is a necessary condition only when combined
	// directly with AND/OR. Other operators can invert its truth value.
	if o.Op != OpContainsSequence && o.Op != OpAnd && o.Op != OpOr {
		start := len(request.Conditions)
		defer degradeSubstringConditions(request, start, false)
	}
	// TODO we can further optimise this by attempting to execute every FieldExpression, if they only contain statics it should resolve
	switch l := o.LHS.(type) {
	case Attribute:
		switch r := o.RHS.(type) {
		case Static:
			switch {
			case (o.RHS.(Static).Type == TypeNil && o.Op == OpNotEqual) || !o.Op.isBoolean(): // the fetch layer can't build predicates on operators that are not boolean
				request.appendCondition(Condition{
					Attribute: l,
					Op:        OpNone,
					Operands:  nil,
				})
			case r.Type == TypeBoolean && (o.Op == OpOr || o.Op == OpAnd):
				request.appendCondition(Condition{
					Attribute: l,
					Op:        OpNone,
					Operands:  nil,
				})
			default:
				request.appendCondition(Condition{
					Attribute: l,
					Op:        o.Op,
					Operands:  []Static{r},
				})
			}
		case Attribute:
			// Both sides are attributes, just fetch both
			request.appendCondition(Condition{
				Attribute: o.LHS.(Attribute),
				Op:        OpNone,
				Operands:  nil,
			})
			request.appendCondition(Condition{
				Attribute: o.RHS.(Attribute),
				Op:        OpNone,
				Operands:  nil,
			})
		default:
			// Just fetch LHS and try to do something smarter with RHS
			request.appendCondition(Condition{
				Attribute: o.LHS.(Attribute),
				Op:        OpNone,
				Operands:  nil,
			})
			o.RHS.extractConditions(request)
		}
	case Static:
		switch r := o.RHS.(type) {
		case Static:
			return // if both are Static, don't need to send any conditions
		case Attribute:
			switch {
			case (o.LHS.(Static).Type == TypeNil && o.Op == OpNotEqual) || !o.Op.isBoolean(): // the fetch layer can't build predicates on operators that are not boolean
				request.appendCondition(Condition{
					Attribute: r,
					Op:        OpNone,
					Operands:  nil,
				})
			case l.Type == TypeBoolean && (o.Op == OpOr || o.Op == OpAnd):
				request.appendCondition(Condition{
					Attribute: r,
					Op:        OpNone,
					Operands:  nil,
				})
			default:
				request.appendCondition(Condition{
					Attribute: r,
					Op:        o.Op,
					Operands:  []Static{l},
				})
			}
		default:
			o.RHS.extractConditions(request)
		}
	default:
		o.LHS.extractConditions(request)
		o.RHS.extractConditions(request)
		request.AllConditions = request.AllConditions && (o.Op != OpOr)
	}
}

func (o UnaryOperation) extractConditions(request *FetchSpansRequest) {
	if o.Op == OpNot {
		start := len(request.Conditions)
		o.Expression.extractConditions(request)
		// Negation can reverse any predicate, so fetch the attribute unfiltered.
		degradeSubstringConditions(request, start, true)
		return
	}

	switch expr := o.Expression.(type) {
	case Attribute:
		switch o.Op {
		case OpExists, OpNotExists:
			request.appendCondition(Condition{
				Attribute: expr,
				Op:        o.Op,
				Operands:  nil,
			})
			return
		default:
			expr.extractConditions(request)
		}
	default:
		expr.extractConditions(request)
	}
}

// An inverted substring predicate must not restrict candidates, including
// spans without a sidecar. Fetch all spans and still load the sidecar when present.
func degradeSubstringConditions(request *FetchSpansRequest, start int, all bool) {
	found := false
	for i := start; i < len(request.Conditions); i++ {
		if request.Conditions[i].Op == OpContainsSequence {
			found = true
		}
		if all || request.Conditions[i].Op == OpContainsSequence {
			request.Conditions[i].Op = OpNone
			request.Conditions[i].Operands = nil
		}
	}
	if found {
		request.AllConditions = false
		request.appendCondition(Condition{Attribute: NewIntrinsic(IntrinsicSpanStartTime), Op: OpNone})
	}
}

func (s Static) extractConditions(*FetchSpansRequest) {
}

func (a Attribute) extractConditions(request *FetchSpansRequest) {
	request.appendCondition(Condition{
		Attribute: a,
		Op:        OpNone,
		Operands:  nil,
	})
}

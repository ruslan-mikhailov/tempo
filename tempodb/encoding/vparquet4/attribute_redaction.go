package vparquet4

import (
	"github.com/parquet-go/parquet-go"

	"github.com/grafana/tempo/v3/tempodb/backend"
	"github.com/grafana/tempo/v3/tempodb/encoding/common"
)

// redactAttributeRow reconstructs only during attribute redaction; unchanged rows
// are written directly rather than deconstructed again.
func redactAttributeRow(schema *parquet.Schema, row parquet.Row, rule *common.AttributeRedactionRule, columns backend.DedicatedColumns, apply bool) (parquet.Row, bool, error) {
	if !rule.RowMightMatch(row) {
		return row, false, nil
	}
	var trace Trace
	if err := schema.Reconstruct(&trace, row); err != nil {
		return nil, false, err
	}
	if !rule.MatchesTime(trace.StartTimeUnixNano, trace.EndTimeUnixNano) {
		return row, false, nil
	}
	if len(rule.Pairs) != 0 {
		return redactAttributePairs(schema, row, &trace, rule.Pairs, columns, apply)
	}
	if pair, ok := rule.LegacySidecar(); ok {
		return redactAttributePairs(schema, row, &trace, []common.AttributeRedactionPair{pair}, columns, apply)
	}
	changed := false
	for i := range trace.ResourceSpans {
		rs := &trace.ResourceSpans[i]
		if rule.Scope == backend.DedicatedColumnScopeResource {
			for j := range rs.Resource.Attrs {
				a := &rs.Resource.Attrs[j]
				if a.Key == rule.Key && !a.IsArray && len(a.Value) == 1 && rule.MatchesValue(a.Value[0]) {
					changed = true
					if apply {
						a.Value[0] = common.RedactedAttributeValue
					}
				}
			}
			var static *string
			switch rule.Key {
			case LabelServiceName:
				static = &rs.Resource.ServiceName
			case LabelCluster:
				static = rs.Resource.Cluster
			case LabelNamespace:
				static = rs.Resource.Namespace
			case LabelPod:
				static = rs.Resource.Pod
			case LabelContainer:
				static = rs.Resource.Container
			case LabelK8sClusterName:
				static = rs.Resource.K8sClusterName
			case LabelK8sNamespaceName:
				static = rs.Resource.K8sNamespaceName
			case LabelK8sPodName:
				static = rs.Resource.K8sPodName
			case LabelK8sContainerName:
				static = rs.Resource.K8sContainerName
			}
			if static != nil && rule.MatchesValue(*static) {
				changed = true
				if apply {
					*static = common.RedactedAttributeValue
				}
			}
			dedicated, err := rule.RedactDedicatedString(&rs.Resource.DedicatedAttributes, columns, apply)
			if err != nil {
				return nil, false, err
			}
			changed = changed || dedicated
		} else {
			for j := range rs.ScopeSpans {
				for k := range rs.ScopeSpans[j].Spans {
					span := &rs.ScopeSpans[j].Spans[k]
					for a := range span.Attrs {
						attr := &span.Attrs[a]
						if attr.Key == rule.Key && !attr.IsArray && len(attr.Value) == 1 && rule.MatchesValue(attr.Value[0]) {
							changed = true
							if apply {
								attr.Value[0] = common.RedactedAttributeValue
							}
						}
					}
					var static *string
					switch rule.Key {
					case LabelHTTPMethod:
						static = span.HttpMethod
					case LabelHTTPUrl:
						static = span.HttpUrl
					}
					if static != nil && rule.MatchesValue(*static) {
						changed = true
						if apply {
							*static = common.RedactedAttributeValue
						}
					}
					dedicated, err := rule.RedactDedicatedString(&span.DedicatedAttributes, columns, apply)
					if err != nil {
						return nil, false, err
					}
					changed = changed || dedicated
				}
			}
		}
	}
	if changed && apply && rule.Scope == backend.DedicatedColumnScopeResource && rule.Key == LabelServiceName {
		if rule.MatchesValue(trace.RootServiceName) {
			trace.RootServiceName = common.RedactedAttributeValue
		}
		for name, stats := range trace.ServiceStats {
			if !rule.MatchesValue(name) {
				continue
			}
			delete(trace.ServiceStats, name)
			existing := trace.ServiceStats[common.RedactedAttributeValue]
			existing.SpanCount += stats.SpanCount
			existing.ErrorCount += stats.ErrorCount
			trace.ServiceStats[common.RedactedAttributeValue] = existing
		}
	}
	if changed && apply {
		return schema.Deconstruct(nil, &trace), true, nil
	}
	return row, changed, nil
}

func pairedAttrKey(a *Attribute) string { return a.Key }

func pairedEncValue(a *Attribute) string {
	if !a.IsArray && len(a.Value) == 1 {
		return a.Value[0]
	}
	return ""
}

func pairedSidecarMatches(a *Attribute, pair *common.AttributeRedactionPair) bool {
	for _, v := range a.Value {
		if pair.MatchesSidecar(v) {
			return true
		}
	}
	return false
}

func redactPairedAttr(a *Attribute) { a.Value[0] = common.RedactedAttributeValue }

func redactAttributePairs(schema *parquet.Schema, row parquet.Row, trace *Trace, pairs []common.AttributeRedactionPair, columns backend.DedicatedColumns, apply bool) (parquet.Row, bool, error) {
	changed := false
	for i := range trace.ResourceSpans {
		rs := &trace.ResourceSpans[i]
		for j := range pairs {
			pair := &pairs[j]
			r := &pair.Enc
			if r.Scope == backend.DedicatedColumnScopeResource {
				dedicated, err := r.RedactDedicatedString(&rs.Resource.DedicatedAttributes, columns, apply)
				if err != nil {
					return nil, false, err
				}
				var matched bool
				rs.Resource.Attrs, matched = common.RedactPairAttributes(rs.Resource.Attrs, pair, pairedAttrKey, pairedEncValue, pairedSidecarMatches, redactPairedAttr, apply, dedicated)
				changed = changed || matched
			} else {
				for k := range rs.ScopeSpans {
					for s := range rs.ScopeSpans[k].Spans {
						span := &rs.ScopeSpans[k].Spans[s]
						dedicated, err := r.RedactDedicatedString(&span.DedicatedAttributes, columns, apply)
						if err != nil {
							return nil, false, err
						}
						var matched bool
						span.Attrs, matched = common.RedactPairAttributes(span.Attrs, pair, pairedAttrKey, pairedEncValue, pairedSidecarMatches, redactPairedAttr, apply, dedicated)
						changed = changed || matched
					}
				}
			}
		}
	}
	if changed && apply {
		return schema.Deconstruct(nil, trace), true, nil
	}
	return row, changed, nil
}

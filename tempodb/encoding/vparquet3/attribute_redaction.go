package vparquet3

import (
	"github.com/parquet-go/parquet-go"

	"github.com/grafana/tempo/v3/tempodb/backend"
	"github.com/grafana/tempo/v3/tempodb/encoding/common"
)

// redactAttributeRow reconstructs a trace only in the attribute-redaction path.
// Unchanged rows are passed to the block writer verbatim.
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
	changed := false
	for i := range trace.ResourceSpans {
		rs := &trace.ResourceSpans[i]
		if rule.Scope == backend.DedicatedColumnScopeResource {
			for j := range rs.Resource.Attrs {
				a := &rs.Resource.Attrs[j]
				if a.Key == rule.Key && a.Value != nil && rule.MatchesValue(*a.Value) {
					changed = true
					if apply {
						*a.Value = common.RedactedAttributeValue
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
						if attr.Key == rule.Key && attr.Value != nil && rule.MatchesValue(*attr.Value) {
							changed = true
							if apply {
								*attr.Value = common.RedactedAttributeValue
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
	if changed && apply && rule.Scope == backend.DedicatedColumnScopeResource &&
		rule.Key == LabelServiceName && rule.MatchesValue(trace.RootServiceName) {
		trace.RootServiceName = common.RedactedAttributeValue
	}
	if changed && apply {
		return schema.Deconstruct(nil, &trace), true, nil
	}
	return row, changed, nil
}

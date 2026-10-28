package tempopb

import (
	"math"
	"strings"
	"testing"

	"github.com/gogo/protobuf/jsonpb"
	commonv1 "github.com/grafana/tempo/v3/pkg/tempopb/common/v1"
	resourcev1 "github.com/grafana/tempo/v3/pkg/tempopb/resource/v1"
	tracev1 "github.com/grafana/tempo/v3/pkg/tempopb/trace/v1"
	"github.com/stretchr/testify/require"
)

func TestMarshalToJSONV1AnyValues(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value *commonv1.AnyValue
		json  string
	}{
		{"unset", &commonv1.AnyValue{}, `{}`},
		{"empty string", &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{}}, `{"stringValue":""}`},
		{"escaped string", &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "\"\\\n<>&\u2028\u2029\xff"}}, `{"stringValue":"\"\\\n\u003c\u003e\u0026\u2028\u2029\ufffd"}`},
		{"invalid UTF8 run", &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "\xff\xfe\xc0"}}, `{"stringValue":"\ufffd\ufffd\ufffd"}`},
		{"false", &commonv1.AnyValue{Value: &commonv1.AnyValue_BoolValue{}}, `{"boolValue":false}`},
		{"true", &commonv1.AnyValue{Value: &commonv1.AnyValue_BoolValue{BoolValue: true}}, `{"boolValue":true}`},
		{"zero integer", &commonv1.AnyValue{Value: &commonv1.AnyValue_IntValue{}}, `{"intValue":"0"}`},
		{"minimum integer", &commonv1.AnyValue{Value: &commonv1.AnyValue_IntValue{IntValue: math.MinInt64}}, `{"intValue":"-9223372036854775808"}`},
		{"maximum integer", &commonv1.AnyValue{Value: &commonv1.AnyValue_IntValue{IntValue: math.MaxInt64}}, `{"intValue":"9223372036854775807"}`},
		{"zero double", &commonv1.AnyValue{Value: &commonv1.AnyValue_DoubleValue{}}, `{"doubleValue":0}`},
		{"negative zero", &commonv1.AnyValue{Value: &commonv1.AnyValue_DoubleValue{DoubleValue: math.Copysign(0, -1)}}, `{"doubleValue":-0}`},
		{"double", &commonv1.AnyValue{Value: &commonv1.AnyValue_DoubleValue{DoubleValue: 1.234e-7}}, `{"doubleValue":1.234e-7}`},
		{"NaN", &commonv1.AnyValue{Value: &commonv1.AnyValue_DoubleValue{DoubleValue: math.NaN()}}, `{"doubleValue":"NaN"}`},
		{"positive infinity", &commonv1.AnyValue{Value: &commonv1.AnyValue_DoubleValue{DoubleValue: math.Inf(1)}}, `{"doubleValue":"Infinity"}`},
		{"negative infinity", &commonv1.AnyValue{Value: &commonv1.AnyValue_DoubleValue{DoubleValue: math.Inf(-1)}}, `{"doubleValue":"-Infinity"}`},
		{"nil bytes", &commonv1.AnyValue{Value: &commonv1.AnyValue_BytesValue{}}, `{"bytesValue":null}`},
		{"empty bytes", &commonv1.AnyValue{Value: &commonv1.AnyValue_BytesValue{BytesValue: []byte{}}}, `{"bytesValue":""}`},
		{"bytes", &commonv1.AnyValue{Value: &commonv1.AnyValue_BytesValue{BytesValue: []byte{0, 1, 254, 255}}}, `{"bytesValue":"AAH+/w=="}`},
		{"nil array", &commonv1.AnyValue{Value: &commonv1.AnyValue_ArrayValue{}}, `{"arrayValue":null}`},
		{"unset array values", &commonv1.AnyValue{Value: &commonv1.AnyValue_ArrayValue{ArrayValue: &commonv1.ArrayValue{}}}, `{"arrayValue":{}}`},
		{"empty array", &commonv1.AnyValue{Value: &commonv1.AnyValue_ArrayValue{ArrayValue: &commonv1.ArrayValue{Values: []*commonv1.AnyValue{}}}}, `{"arrayValue":{"values":[]}}`},
		{"nested array", &commonv1.AnyValue{Value: &commonv1.AnyValue_ArrayValue{ArrayValue: &commonv1.ArrayValue{Values: []*commonv1.AnyValue{nil, {Value: &commonv1.AnyValue_IntValue{IntValue: 42}}}}}}, `{"arrayValue":{"values":[null,{"intValue":"42"}]}}`},
		{"nil kvlist", &commonv1.AnyValue{Value: &commonv1.AnyValue_KvlistValue{}}, `{"kvlistValue":null}`},
		{"empty kvlist", &commonv1.AnyValue{Value: &commonv1.AnyValue_KvlistValue{KvlistValue: &commonv1.KeyValueList{Values: []*commonv1.KeyValue{}}}}, `{"kvlistValue":{"values":[]}}`},
		{"nested kvlist", &commonv1.AnyValue{Value: &commonv1.AnyValue_KvlistValue{KvlistValue: &commonv1.KeyValueList{Values: []*commonv1.KeyValue{nil, {Key: "nested", Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_BoolValue{}}}}}}}, `{"kvlistValue":{"values":[null,{"key":"nested","value":{"boolValue":false}}]}}`},
		{"zero string index", &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValueStrindex{}}, `{"stringValueStrindex":0}`},
		{"string index", &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValueStrindex{StringValueStrindex: 42}}, `{"stringValueStrindex":42}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			trace := &Trace{ResourceSpans: []*tracev1.ResourceSpans{{Resource: &resourcev1.Resource{
				Attributes: []*commonv1.KeyValue{{Key: "test", Value: tc.value}},
			}}}}
			got, err := MarshalToJSONV1(trace)
			require.NoError(t, err)
			require.JSONEq(t, `{"batches":[{"resource":{"attributes":[{"key":"test","value":`+tc.json+`}]}}]}`, string(got))
			if value, ok := tc.value.Value.(*commonv1.AnyValue_DoubleValue); ok && !math.IsNaN(value.DoubleValue) {
				var decoded Trace
				require.NoError(t, UnmarshalFromJSONV1(got, &decoded))
				roundtrip := decoded.ResourceSpans[0].Resource.Attributes[0].Value.GetDoubleValue()
				require.Equal(t, math.Float64bits(value.DoubleValue), math.Float64bits(roundtrip))
			}
		})
	}
}

func TestMarshalToJSONV1NestedFields(t *testing.T) {
	attributes := []*commonv1.KeyValue{nil, {Key: "key", KeyStrindex: 3, Value: &commonv1.AnyValue{}}}
	for _, unknownEnums := range []bool{false, true} {
		t.Run(map[bool]string{false: "known enums", true: "unknown enums"}[unknownEnums], func(t *testing.T) {
			span := &tracev1.Span{
				TraceId: []byte{1, 2, 3}, SpanId: []byte{}, ParentSpanId: []byte{4}, TraceState: "trace-state", Flags: math.MaxUint32,
				Name: "span", Kind: tracev1.Span_SPAN_KIND_SERVER, StartTimeUnixNano: math.MaxUint64, EndTimeUnixNano: 1,
				Attributes: attributes, DroppedAttributesCount: 1, DroppedEventsCount: 2, DroppedLinksCount: 3,
				Events: []*tracev1.Span_Event{nil, {TimeUnixNano: math.MaxUint64, Name: "event", Attributes: attributes, DroppedAttributesCount: 4}},
				Links:  []*tracev1.Span_Link{nil, {TraceId: []byte{5}, SpanId: []byte{6}, TraceState: "link-state", Attributes: attributes, DroppedAttributesCount: 5, Flags: 6}},
				Status: &tracev1.Status{Message: "status", Code: tracev1.Status_STATUS_CODE_ERROR},
			}
			if unknownEnums {
				span.Kind = -1
				span.Status.Code = 42
			}
			trace := &Trace{ResourceSpans: []*tracev1.ResourceSpans{nil, {
				Resource: &resourcev1.Resource{
					Attributes: attributes, DroppedAttributesCount: 7,
					EntityRefs: []*commonv1.EntityRef{nil, {SchemaUrl: "entity-schema", Type: "entity", IdKeys: []string{"id"}, DescriptionKeys: []string{}}},
				},
				ScopeSpans: []*tracev1.ScopeSpans{nil, {
					Scope: &commonv1.InstrumentationScope{Name: "scope", Version: "version", Attributes: attributes, DroppedAttributesCount: 8},
					Spans: []*tracev1.Span{nil, span, {Attributes: []*commonv1.KeyValue{}, Events: []*tracev1.Span_Event{}, Links: []*tracev1.Span_Link{}}}, SchemaUrl: "scope-schema",
				}},
				SchemaUrl: "resource-schema",
			}}}
			want, err := new(jsonpb.Marshaler).MarshalToString(trace)
			require.NoError(t, err)
			got, err := MarshalToJSONV1(trace)
			require.NoError(t, err)
			require.JSONEq(t, strings.Replace(want, `"resourceSpans":`, `"batches":`, 1), string(got))
		})
	}
}

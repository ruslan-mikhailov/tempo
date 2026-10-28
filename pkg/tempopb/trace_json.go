package tempopb

import (
	"math"
	"strconv"
	"unicode/utf8"

	"github.com/bytedance/sonic/encoder"
	sonicutf8 "github.com/bytedance/sonic/utf8"
	commonv1 "github.com/grafana/tempo/v3/pkg/tempopb/common/v1"
	resourcev1 "github.com/grafana/tempo/v3/pkg/tempopb/resource/v1"
	tracev1 "github.com/grafana/tempo/v3/pkg/tempopb/trace/v1"
)

// traceJSONWriter appends directly to the result rather than constructing a
// second trace-shaped object graph. Sonic handles scalar JSON encoding; the
// traversal preserves jsonpb's field names, presence rules and numeric types.
// In particular, a non-nil empty repeated field is emitted, not omitted.
type traceJSONWriter struct {
	buf []byte
	err error
}

func (w *traceJSONWriter) field(name string) {
	if w.buf[len(w.buf)-1] != '{' {
		w.buf = append(w.buf, ',')
	}
	w.buf = append(w.buf, '"')
	w.buf = append(w.buf, name...)
	w.buf = append(w.buf, `":`...)
}

func (w *traceJSONWriter) value(v any) {
	if w.err == nil {
		// Escape/validate once at the end. EncodeInto applies these options to
		// the entire buffer, which would otherwise make appending quadratic.
		w.err = encoder.EncodeInto(&w.buf, v, encoder.NoEncoderNewline)
	}
}

func (w *traceJSONWriter) string(name, v string) {
	if v != "" {
		w.field(name)
		w.value(v)
	}
}

func (w *traceJSONWriter) bytes(name string, v []byte) {
	if v != nil {
		w.field(name)
		w.value(v)
	}
}

func (w *traceJSONWriter) uint32(name string, v uint32) {
	if v != 0 {
		w.field(name)
		w.buf = strconv.AppendUint(w.buf, uint64(v), 10)
	}
}

func (w *traceJSONWriter) uint64(name string, v uint64) {
	if v != 0 {
		w.field(name)
		w.buf = append(w.buf, '"')
		w.buf = strconv.AppendUint(w.buf, v, 10)
		w.buf = append(w.buf, '"')
	}
}

func (w *traceJSONWriter) enum(name string, v int32, names map[int32]string) {
	if v != 0 {
		w.field(name)
		if name, ok := names[v]; ok {
			w.value(name)
		} else {
			w.buf = strconv.AppendInt(w.buf, int64(v), 10)
		}
	}
}

func traceJSONMessage[T any](w *traceJSONWriter, name string, v *T, encode func(*traceJSONWriter, *T)) {
	if v != nil {
		w.field(name)
		encode(w, v)
	}
}

func traceJSONMessages[T any](w *traceJSONWriter, name string, values []*T, encode func(*traceJSONWriter, *T)) {
	if values == nil {
		return
	}
	w.field(name)
	w.buf = append(w.buf, '[')
	for i, v := range values {
		if i != 0 {
			w.buf = append(w.buf, ',')
		}
		if v == nil {
			w.buf = append(w.buf, "null"...)
		} else {
			encode(w, v)
		}
	}
	w.buf = append(w.buf, ']')
}

func (w *traceJSONWriter) finish() ([]byte, error) {
	if w.err != nil {
		return nil, w.err
	}
	buf := encoder.HTMLEscape(nil, w.buf)
	if !sonicutf8.Validate(buf) {
		// CorrectWith is unavailable on Sonic's fallback architectures. Replace
		// each invalid byte, as encoding/json does, rather than collapsing runs.
		corrected := make([]byte, 0, len(buf))
		for len(buf) != 0 {
			_, n := utf8.DecodeRune(buf)
			if n == 1 && buf[0] >= utf8.RuneSelf {
				corrected = append(corrected, `\ufffd`...)
			} else {
				corrected = append(corrected, buf[:n]...)
			}
			buf = buf[n:]
		}
		buf = corrected
	}
	return buf, nil
}

func traceJSONResourceSpans(w *traceJSONWriter, v *tracev1.ResourceSpans) {
	w.buf = append(w.buf, '{')
	traceJSONMessage(w, "resource", v.Resource, traceJSONResource)
	traceJSONMessages(w, "scopeSpans", v.ScopeSpans, traceJSONScopeSpans)
	w.string("schemaUrl", v.SchemaUrl)
	w.buf = append(w.buf, '}')
}

func traceJSONResource(w *traceJSONWriter, v *resourcev1.Resource) {
	w.buf = append(w.buf, '{')
	traceJSONMessages(w, "attributes", v.Attributes, traceJSONKeyValue)
	w.uint32("droppedAttributesCount", v.DroppedAttributesCount)
	traceJSONMessages(w, "entityRefs", v.EntityRefs, traceJSONEntityRef)
	w.buf = append(w.buf, '}')
}

func traceJSONEntityRef(w *traceJSONWriter, v *commonv1.EntityRef) {
	w.buf = append(w.buf, '{')
	w.string("schemaUrl", v.SchemaUrl)
	w.string("type", v.Type)
	if v.IdKeys != nil {
		w.field("idKeys")
		w.value(v.IdKeys)
	}
	if v.DescriptionKeys != nil {
		w.field("descriptionKeys")
		w.value(v.DescriptionKeys)
	}
	w.buf = append(w.buf, '}')
}

func traceJSONScopeSpans(w *traceJSONWriter, v *tracev1.ScopeSpans) {
	w.buf = append(w.buf, '{')
	traceJSONMessage(w, "scope", v.Scope, traceJSONScope)
	traceJSONMessages(w, "spans", v.Spans, traceJSONSpan)
	w.string("schemaUrl", v.SchemaUrl)
	w.buf = append(w.buf, '}')
}

func traceJSONScope(w *traceJSONWriter, v *commonv1.InstrumentationScope) {
	w.buf = append(w.buf, '{')
	w.string("name", v.Name)
	w.string("version", v.Version)
	traceJSONMessages(w, "attributes", v.Attributes, traceJSONKeyValue)
	w.uint32("droppedAttributesCount", v.DroppedAttributesCount)
	w.buf = append(w.buf, '}')
}

func traceJSONSpan(w *traceJSONWriter, v *tracev1.Span) {
	w.buf = append(w.buf, '{')
	w.bytes("traceId", v.TraceId)
	w.bytes("spanId", v.SpanId)
	w.string("traceState", v.TraceState)
	w.bytes("parentSpanId", v.ParentSpanId)
	w.uint32("flags", v.Flags)
	w.string("name", v.Name)
	w.enum("kind", int32(v.Kind), tracev1.Span_SpanKind_name)
	w.uint64("startTimeUnixNano", v.StartTimeUnixNano)
	w.uint64("endTimeUnixNano", v.EndTimeUnixNano)
	traceJSONMessages(w, "attributes", v.Attributes, traceJSONKeyValue)
	w.uint32("droppedAttributesCount", v.DroppedAttributesCount)
	traceJSONMessages(w, "events", v.Events, traceJSONEvent)
	w.uint32("droppedEventsCount", v.DroppedEventsCount)
	traceJSONMessages(w, "links", v.Links, traceJSONLink)
	w.uint32("droppedLinksCount", v.DroppedLinksCount)
	traceJSONMessage(w, "status", v.Status, traceJSONStatus)
	w.buf = append(w.buf, '}')
}

func traceJSONEvent(w *traceJSONWriter, v *tracev1.Span_Event) {
	w.buf = append(w.buf, '{')
	w.uint64("timeUnixNano", v.TimeUnixNano)
	w.string("name", v.Name)
	traceJSONMessages(w, "attributes", v.Attributes, traceJSONKeyValue)
	w.uint32("droppedAttributesCount", v.DroppedAttributesCount)
	w.buf = append(w.buf, '}')
}

func traceJSONLink(w *traceJSONWriter, v *tracev1.Span_Link) {
	w.buf = append(w.buf, '{')
	w.bytes("traceId", v.TraceId)
	w.bytes("spanId", v.SpanId)
	w.string("traceState", v.TraceState)
	traceJSONMessages(w, "attributes", v.Attributes, traceJSONKeyValue)
	w.uint32("droppedAttributesCount", v.DroppedAttributesCount)
	w.uint32("flags", v.Flags)
	w.buf = append(w.buf, '}')
}

func traceJSONStatus(w *traceJSONWriter, v *tracev1.Status) {
	w.buf = append(w.buf, '{')
	w.string("message", v.Message)
	w.enum("code", int32(v.Code), tracev1.Status_StatusCode_name)
	w.buf = append(w.buf, '}')
}

func traceJSONKeyValue(w *traceJSONWriter, v *commonv1.KeyValue) {
	w.buf = append(w.buf, '{')
	w.string("key", v.Key)
	traceJSONMessage(w, "value", v.Value, traceJSONAnyValue)
	if v.KeyStrindex != 0 {
		w.field("keyStrindex")
		w.buf = strconv.AppendInt(w.buf, int64(v.KeyStrindex), 10)
	}
	w.buf = append(w.buf, '}')
}

func traceJSONAnyValue(w *traceJSONWriter, v *commonv1.AnyValue) {
	w.buf = append(w.buf, '{')
	// Oneof presence is independent of the selected value's zero value.
	switch v := v.Value.(type) {
	case *commonv1.AnyValue_StringValue:
		w.field("stringValue")
		w.value(v.StringValue)
	case *commonv1.AnyValue_BoolValue:
		w.field("boolValue")
		w.value(v.BoolValue)
	case *commonv1.AnyValue_IntValue:
		w.field("intValue")
		w.buf = append(w.buf, '"')
		w.buf = strconv.AppendInt(w.buf, v.IntValue, 10)
		w.buf = append(w.buf, '"')
	case *commonv1.AnyValue_DoubleValue:
		w.field("doubleValue")
		switch {
		case math.IsNaN(v.DoubleValue):
			w.value("NaN")
		case math.IsInf(v.DoubleValue, 1):
			w.value("Infinity")
		case math.IsInf(v.DoubleValue, -1):
			w.value("-Infinity")
		default:
			// Sonic normalizes negative zero. Preserve it and use the same
			// decimal/exponent cutoffs as encoding/json.
			format := byte('f')
			abs := math.Abs(v.DoubleValue)
			if abs != 0 && (abs < 1e-6 || abs >= 1e21) {
				format = 'e'
			}
			w.buf = strconv.AppendFloat(w.buf, v.DoubleValue, format, -1, 64)
			if format == 'e' {
				n := len(w.buf)
				if w.buf[n-4] == 'e' && w.buf[n-3] == '-' && w.buf[n-2] == '0' {
					w.buf[n-2] = w.buf[n-1]
					w.buf = w.buf[:n-1]
				}
			}
		}
	case *commonv1.AnyValue_ArrayValue:
		w.field("arrayValue")
		if v.ArrayValue == nil {
			w.buf = append(w.buf, "null"...)
		} else {
			w.buf = append(w.buf, '{')
			traceJSONMessages(w, "values", v.ArrayValue.Values, traceJSONAnyValue)
			w.buf = append(w.buf, '}')
		}
	case *commonv1.AnyValue_KvlistValue:
		w.field("kvlistValue")
		if v.KvlistValue == nil {
			w.buf = append(w.buf, "null"...)
		} else {
			w.buf = append(w.buf, '{')
			traceJSONMessages(w, "values", v.KvlistValue.Values, traceJSONKeyValue)
			w.buf = append(w.buf, '}')
		}
	case *commonv1.AnyValue_BytesValue:
		w.field("bytesValue")
		w.value(v.BytesValue)
	case *commonv1.AnyValue_StringValueStrindex:
		w.field("stringValueStrindex")
		w.buf = strconv.AppendInt(w.buf, int64(v.StringValueStrindex), 10)
	}
	w.buf = append(w.buf, '}')
}

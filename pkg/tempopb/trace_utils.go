package tempopb

import (
	"errors"
	"strings"

	"github.com/gogo/protobuf/jsonpb"
)

// MarshalToJSONV1 marshals a trace as protobuf JSON using the legacy "batches"
// field name instead of "resourceSpans".
func MarshalToJSONV1(t *Trace) ([]byte, error) {
	if t == nil {
		return nil, errors.New("Marshal called with nil")
	}
	w := traceJSONWriter{buf: make([]byte, 0, 4096)}
	w.buf = append(w.buf, '{')
	traceJSONMessages(&w, "batches", t.ResourceSpans, traceJSONResourceSpans)
	w.buf = append(w.buf, '}')
	return w.finish()
}

// It unmarshal an OTEL compatible JSON to a Trace.
// Historically, our Trace proto message used `batches` to define the array of resourses spans.
// To be OTEL compatible we renamed it to `resourcesSpan`.
// To be backward compatible, this function replaces the first occurrence of `batches` by `resourcesSpan`
// and then use jsonpb to unmarshal JSON into a Trace.
func UnmarshalFromJSONV1(data []byte, t *Trace) error {
	marshaler := &jsonpb.Unmarshaler{}
	jsonStr := strings.Replace(string(data), `"batches":`, `"resourceSpans":`, 1)
	err := marshaler.Unmarshal(strings.NewReader(jsonStr), t)
	return err
}

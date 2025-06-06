package test

import (
	crand "crypto/rand"
	"encoding/json"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/grafana/tempo/pkg/tempopb"
	v1_common "github.com/grafana/tempo/pkg/tempopb/common/v1"
	v1_resource "github.com/grafana/tempo/pkg/tempopb/resource/v1"
	v1_trace "github.com/grafana/tempo/pkg/tempopb/trace/v1"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/stretchr/testify/require"
)

func MakeAttribute(key, value string) *v1_common.KeyValue {
	return &v1_common.KeyValue{
		Key: key,
		Value: &v1_common.AnyValue{
			Value: &v1_common.AnyValue_StringValue{
				StringValue: value,
			},
		},
	}
}

func MakeSpan(traceID []byte) *v1_trace.Span {
	now := time.Now()
	startTime := uint64(now.UnixNano())
	endTime := uint64(now.Add(time.Second).UnixNano())
	return makeSpanWithAttributeCount(traceID, rand.Int()%10+1, startTime, endTime)
}

func MakeSpanWithTimeWindow(traceID []byte, startTime uint64, endTime uint64) *v1_trace.Span {
	return makeSpanWithAttributeCount(traceID, rand.Int()%10+1, startTime, endTime)
}

func makeSpanWithAttributeCount(traceID []byte, count int, startTime uint64, endTime uint64) *v1_trace.Span {
	attributes := make([]*v1_common.KeyValue, 0, count)
	for i := 0; i < count; i++ {
		attributes = append(attributes, &v1_common.KeyValue{
			Key:   RandomString(),
			Value: &v1_common.AnyValue{Value: &v1_common.AnyValue_StringValue{StringValue: RandomString()}},
		})
	}
	s := &v1_trace.Span{
		Name:         "test",
		TraceId:      traceID,
		SpanId:       make([]byte, 8),
		ParentSpanId: make([]byte, 8),
		Kind:         v1_trace.Span_SPAN_KIND_CLIENT,
		Status: &v1_trace.Status{
			Code:    1,
			Message: "OK",
		},
		StartTimeUnixNano:      startTime,
		EndTimeUnixNano:        endTime,
		Attributes:             attributes,
		DroppedLinksCount:      rand.Uint32(),
		DroppedAttributesCount: rand.Uint32(),
	}
	_, err := crand.Read(s.SpanId)
	if err != nil {
		panic(err)
	}

	// add link
	if rand.Intn(5) == 0 {
		s.Links = append(s.Links, &v1_trace.Span_Link{
			TraceId:    traceID,
			SpanId:     make([]byte, 8),
			TraceState: "state",
			Attributes: []*v1_common.KeyValue{
				{
					Key: "linkkey",
					Value: &v1_common.AnyValue{
						Value: &v1_common.AnyValue_StringValue{
							StringValue: "linkvalue",
						},
					},
				},
			},
		})
	}

	// add attr
	if rand.Intn(2) == 0 {
		s.Attributes = append(s.Attributes, &v1_common.KeyValue{
			Key: "key",
			Value: &v1_common.AnyValue{
				Value: &v1_common.AnyValue_StringValue{
					StringValue: "value",
				},
			},
		})
	}

	// add event
	if rand.Intn(3) == 0 {
		s.Events = append(s.Events, &v1_trace.Span_Event{
			TimeUnixNano:           s.StartTimeUnixNano + uint64(rand.Intn(1*1000*1000)), // 1ms
			Name:                   "event",
			DroppedAttributesCount: rand.Uint32(),
			Attributes: []*v1_common.KeyValue{
				{
					Key: "eventkey",
					Value: &v1_common.AnyValue{
						Value: &v1_common.AnyValue_StringValue{
							StringValue: "eventvalue",
						},
					},
				},
			},
		})
	}

	return s
}

func MakeBatch(spans int, traceID []byte) *v1_trace.ResourceSpans {
	return makeBatchWithTimeRange(spans, traceID, nil)
}

type batchTimeRange struct {
	start uint64
	end   uint64
}

func makeBatchWithTimeRange(spans int, traceID []byte, timeRange *batchTimeRange) *v1_trace.ResourceSpans {
	traceID = ValidTraceID(traceID)

	batch := &v1_trace.ResourceSpans{
		Resource: &v1_resource.Resource{
			Attributes: []*v1_common.KeyValue{
				{
					Key: "random.res.attr",
					Value: &v1_common.AnyValue{
						Value: &v1_common.AnyValue_StringValue{
							StringValue: RandomString(),
						},
					},
				},
				{
					Key: "service.name",
					Value: &v1_common.AnyValue{
						Value: &v1_common.AnyValue_StringValue{
							StringValue: "test-service",
						},
					},
				},
			},
		},
	}

	var (
		ss      *v1_trace.ScopeSpans
		ssCount int
	)

	for i := 0; i < spans; i++ {
		// occasionally make a new ss
		if ss == nil || rand.Int()%3 == 0 {
			ssCount++
			ss = &v1_trace.ScopeSpans{
				Scope: &v1_common.InstrumentationScope{
					Name:    "super library",
					Version: fmt.Sprintf("1.0.%d", ssCount),
				},
			}

			batch.ScopeSpans = append(batch.ScopeSpans, ss)
		}

		if timeRange == nil {
			ss.Spans = append(ss.Spans, MakeSpan(traceID))
		} else {
			ss.Spans = append(ss.Spans, MakeSpanWithTimeWindow(traceID, timeRange.start, timeRange.end))
		}
	}
	return batch
}

func MakeTrace(requests int, traceID []byte) *tempopb.Trace {
	traceID = ValidTraceID(traceID)

	trace := &tempopb.Trace{
		ResourceSpans: make([]*v1_trace.ResourceSpans, 0),
	}

	for i := 0; i < requests; i++ {
		trace.ResourceSpans = append(trace.ResourceSpans, MakeBatch(rand.Int()%20+1, traceID))
	}

	return trace
}

func MakeTraceWithTimeRange(requests int, traceID []byte, startTime, endTime uint64) *tempopb.Trace {
	traceID = ValidTraceID(traceID)

	trace := &tempopb.Trace{
		ResourceSpans: make([]*v1_trace.ResourceSpans, 0),
	}

	for i := 0; i < requests; i++ {
		timeRange := &batchTimeRange{start: startTime, end: endTime}
		trace.ResourceSpans = append(trace.ResourceSpans, makeBatchWithTimeRange(rand.Int()%20+1, traceID, timeRange))
	}

	return trace
}

func MakeTraceWithSpanCount(requests int, spansEach int, traceID []byte) *tempopb.Trace {
	trace := &tempopb.Trace{
		ResourceSpans: make([]*v1_trace.ResourceSpans, 0),
	}

	for i := 0; i < requests; i++ {
		trace.ResourceSpans = append(trace.ResourceSpans, MakeBatch(spansEach, traceID))
	}

	return trace
}

var (
	dedicatedColumnsResource = backend.DedicatedColumns{
		{Scope: "resource", Name: "dedicated.resource.1", Type: "string"},
		{Scope: "resource", Name: "dedicated.resource.2", Type: "string"},
		{Scope: "resource", Name: "dedicated.resource.3", Type: "string"},
		{Scope: "resource", Name: "dedicated.resource.4", Type: "string"},
		{Scope: "resource", Name: "dedicated.resource.5", Type: "string"},
	}
	dedicatedColumnsSpan = backend.DedicatedColumns{
		{Scope: "span", Name: "dedicated.span.1", Type: "string"},
		{Scope: "span", Name: "dedicated.span.2", Type: "string"},
		{Scope: "span", Name: "dedicated.span.3", Type: "string"},
		{Scope: "span", Name: "dedicated.span.4", Type: "string"},
		{Scope: "span", Name: "dedicated.span.5", Type: "string"},
	}
)

// AddDedicatedAttributes adds resource and span attributes to a trace that are stored in dedicated
// columns when a backend.BlockMeta is created with the column assignments from MakeDedicatedColumns.
func AddDedicatedAttributes(trace *tempopb.Trace) *tempopb.Trace {
	spanAttrs := make([]*v1_common.KeyValue, 0, len(dedicatedColumnsSpan))
	for i, c := range dedicatedColumnsSpan {
		spanAttrs = append(spanAttrs, &v1_common.KeyValue{
			Key: c.Name,
			Value: &v1_common.AnyValue{
				Value: &v1_common.AnyValue_StringValue{
					StringValue: fmt.Sprintf("dedicated-span-attr-value-%d", i+1),
				},
			},
		})
	}
	resourceAttrs := make([]*v1_common.KeyValue, 0, len(dedicatedColumnsResource))
	for i, c := range dedicatedColumnsResource {
		resourceAttrs = append(resourceAttrs, &v1_common.KeyValue{
			Key: c.Name,
			Value: &v1_common.AnyValue{
				Value: &v1_common.AnyValue_StringValue{
					StringValue: fmt.Sprintf("dedicated-resource-attr-value-%d", i+1),
				},
			},
		})
	}

	for _, batch := range trace.ResourceSpans {
		attr := make([]*v1_common.KeyValue, 0, len(resourceAttrs)+len(batch.Resource.Attributes))
		attr = append(attr, resourceAttrs...)
		batch.Resource.Attributes = append(attr, batch.Resource.Attributes...)

		for _, ss := range batch.ScopeSpans {
			for _, span := range ss.Spans {
				attr = make([]*v1_common.KeyValue, 0, len(spanAttrs)+len(span.Attributes))
				attr = append(attr, spanAttrs...)
				span.Attributes = append(attr, span.Attributes...)
			}
		}
	}

	return trace
}

func MakeReqWithMultipleTraceWithSpanCount(spanCounts []int, traceIDs [][]byte) *tempopb.Trace {
	if len(spanCounts) != len(traceIDs) {
		panic("spanCounts and traceIDs lengths do not match")
	}
	trace := &tempopb.Trace{
		ResourceSpans: make([]*v1_trace.ResourceSpans, 0),
	}

	for index, traceID := range traceIDs {
		traceID = ValidTraceID(traceID)
		trace.ResourceSpans = append(trace.ResourceSpans, MakeBatch(spanCounts[index], traceID))
	}

	return trace
}

// MakeDedicatedColumns creates a dedicated column assignment that matches the attributes
// generated by AddDedicatedAttributes.
func MakeDedicatedColumns() backend.DedicatedColumns {
	columns := make(backend.DedicatedColumns, 0, len(dedicatedColumnsResource)+len(dedicatedColumnsSpan))
	columns = append(columns, dedicatedColumnsResource...)
	columns = append(columns, dedicatedColumnsSpan...)
	return columns
}

func ValidTraceID(traceID []byte) []byte {
	if len(traceID) == 0 {
		traceID = make([]byte, 16)
		_, err := crand.Read(traceID)
		if err != nil {
			panic(err)
		}
	}

	for len(traceID) < 16 {
		traceID = append(traceID, 0)
	}

	return traceID
}

func RandomString() string {
	letters := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

	s := make([]rune, 10)
	for i := range s {
		s[i] = letters[rand.Intn(len(letters))]
	}
	return string(s)
}

func TracesEqual(t *testing.T, t1 *tempopb.Trace, t2 *tempopb.Trace) {
	if !proto.Equal(t1, t2) {
		wantJSON, _ := json.MarshalIndent(t1, "", "  ")
		gotJSON, _ := json.MarshalIndent(t2, "", "  ")

		require.Equal(t, string(wantJSON), string(gotJSON))
	}
}

func MakeTraceWithTags(traceID []byte, service string, intValue int64) *tempopb.Trace {
	now := time.Now()

	traceID = ValidTraceID(traceID)

	trace := &tempopb.Trace{
		ResourceSpans: make([]*v1_trace.ResourceSpans, 0),
	}

	attributes := make([]*v1_common.KeyValue, 0, 2)
	attributes = append(attributes, &v1_common.KeyValue{
		Key:   "stringTag",
		Value: &v1_common.AnyValue{Value: &v1_common.AnyValue_StringValue{StringValue: "value1"}},
	})

	attributes = append(attributes, &v1_common.KeyValue{
		Key:   "intTag",
		Value: &v1_common.AnyValue{Value: &v1_common.AnyValue_IntValue{IntValue: intValue}},
	})

	trace.ResourceSpans = append(trace.ResourceSpans, &v1_trace.ResourceSpans{
		Resource: &v1_resource.Resource{
			Attributes: []*v1_common.KeyValue{
				{
					Key: "service.name",
					Value: &v1_common.AnyValue{
						Value: &v1_common.AnyValue_StringValue{
							StringValue: service,
						},
					},
				},
				{
					Key: "other",
					Value: &v1_common.AnyValue{
						Value: &v1_common.AnyValue_StringValue{
							StringValue: "other-value",
						},
					},
				},
			},
		},
		ScopeSpans: []*v1_trace.ScopeSpans{
			{
				Spans: []*v1_trace.Span{
					{
						Name:         "test",
						TraceId:      traceID,
						SpanId:       make([]byte, 8),
						ParentSpanId: make([]byte, 8),
						Kind:         v1_trace.Span_SPAN_KIND_CLIENT,
						Status: &v1_trace.Status{
							Code:    1,
							Message: "OK",
						},
						StartTimeUnixNano:      uint64(now.UnixNano()),
						EndTimeUnixNano:        uint64(now.Add(time.Second).UnixNano()),
						Attributes:             attributes,
						DroppedLinksCount:      rand.Uint32(),
						DroppedAttributesCount: rand.Uint32(),
					},
				},
			},
		},
	})
	return trace
}

func MakePushBytesRequest(t testing.TB, requests int, traceID []byte, startTime, endTime uint64) *tempopb.PushBytesRequest {
	traceID = ValidTraceID(traceID)
	trace := MakeTraceWithTimeRange(requests, traceID, startTime, endTime)
	b, err := proto.Marshal(trace)
	require.NoError(t, err)

	req := &tempopb.PushBytesRequest{
		Traces: make([]tempopb.PreallocBytes, 0),
		Ids:    make([][]byte, 0),
	}
	req.Traces = append(req.Traces, tempopb.PreallocBytes{Slice: b})
	req.Ids = append(req.Ids, traceID)

	return req
}

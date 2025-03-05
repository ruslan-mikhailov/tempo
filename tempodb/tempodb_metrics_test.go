package tempodb

import (
	"context"
	"math"
	"path"
	"sort"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/google/go-cmp/cmp"
	"github.com/grafana/tempo/pkg/model"
	"github.com/grafana/tempo/pkg/tempopb"
	common_v1 "github.com/grafana/tempo/pkg/tempopb/common/v1"
	resource_v1 "github.com/grafana/tempo/pkg/tempopb/resource/v1"
	v1 "github.com/grafana/tempo/pkg/tempopb/trace/v1"
	"github.com/grafana/tempo/pkg/traceql"
	"github.com/grafana/tempo/pkg/util/test"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/backend/local"
	"github.com/grafana/tempo/tempodb/encoding/common"
	"github.com/grafana/tempo/tempodb/encoding/vparquet4"
	"github.com/grafana/tempo/tempodb/wal"
	"github.com/stretchr/testify/require"
)

func requestWithDefaultRange(q string) *tempopb.QueryRangeRequest {
	return &tempopb.QueryRangeRequest{
		Start: 1,
		End:   50 * uint64(time.Second),
		Step:  15 * uint64(time.Second),
		Query: q,
	}
}

var queryRangeTestCases = []struct {
	name     string
	req      *tempopb.QueryRangeRequest
	expected []*tempopb.TimeSeries
}{
	{
		name: "rate",
		req:  requestWithDefaultRange("{ } | rate()"),
		expected: []*tempopb.TimeSeries{
			{
				PromLabels: `{__name__="rate"}`,
				Labels:     []common_v1.KeyValue{tempopb.MakeKeyValueString("__name__", "rate")},
				Samples: []tempopb.Sample{
					{TimestampMs: 0, Value: 14.0 / 15.0},     // First interval starts at 1, so it only has 14 spans
					{TimestampMs: 15_000, Value: 1.0},        // Spans every 1 second
					{TimestampMs: 30_000, Value: 1.0},        // Spans every 1 second
					{TimestampMs: 45_000, Value: 5.0 / 15.0}, // Interval [45,50) has 5 spans
					{TimestampMs: 60_000, Value: 0},          // I think this is a bug that we extend out an extra interval
				},
			},
		},
	},
	{
		name: "rate_with_filter",
		req:  requestWithDefaultRange(`{ .service.name="even" } | rate()`),
		expected: []*tempopb.TimeSeries{
			{
				PromLabels: `{__name__="rate"}`,
				Labels:     []common_v1.KeyValue{tempopb.MakeKeyValueString("__name__", "rate")},
				Samples: []tempopb.Sample{
					{TimestampMs: 0, Value: 7.0 / 15.0},      // Interval [ 1, 14], 7 spans at 2, 4, 6, 8, 10, 12, 14
					{TimestampMs: 15_000, Value: 7.0 / 15.0}, // Interval [15, 29], 7 spans at 16, 18, 20, 22, 24, 26, 28
					{TimestampMs: 30_000, Value: 8.0 / 15.0}, // Interval [30, 44], 8 spans at 30, 32, 34, 36, 38, 40, 42, 44
					{TimestampMs: 45_000, Value: 2.0 / 15.0}, // Interval [45, 50), 2 spans at 46, 48
					{TimestampMs: 60_000, Value: 0},          // I think this is a bug that we extend out an extra interval
				},
			},
		},
	},
	{
		name: "rate_no_spans",
		req:  requestWithDefaultRange(`{ .service.name="does_not_exist" } | rate()`),
		expected: []*tempopb.TimeSeries{
			{
				PromLabels: `{__name__="rate"}`,
				Labels:     []common_v1.KeyValue{tempopb.MakeKeyValueString("__name__", "rate")},
				Samples: []tempopb.Sample{
					{TimestampMs: 0, Value: 0},
					{TimestampMs: 15_000, Value: 0},
					{TimestampMs: 30_000, Value: 0},
					{TimestampMs: 45_000, Value: 0},
					{TimestampMs: 60_000, Value: 0},
				},
			},
		},
	},
	{
		name: "count_over_time",
		req:  requestWithDefaultRange(`{ } | count_over_time()`),
		expected: []*tempopb.TimeSeries{
			{
				PromLabels: `{__name__="count_over_time"}`,
				Labels:     []common_v1.KeyValue{tempopb.MakeKeyValueString("__name__", "count_over_time")},
				Samples: []tempopb.Sample{
					{TimestampMs: 0, Value: 14},      // Interval [1, 14], 14 spans
					{TimestampMs: 15_000, Value: 15}, // Interval [15, 29], 15 spans
					{TimestampMs: 30_000, Value: 15}, // Interval [30, 44], 15 spans
					{TimestampMs: 45_000, Value: 5},  // Interval [45, 50), 5 spans
					{TimestampMs: 60_000, Value: 0},
				},
			},
		},
	},
	{
		name: "count_over_time",
		req:  requestWithDefaultRange(`{ } | count_over_time() by (.service.name)`),
		expected: []*tempopb.TimeSeries{
			{
				PromLabels: `{.service.name="even"}`,
				Labels:     []common_v1.KeyValue{tempopb.MakeKeyValueString(".service.name", "even")},
				Samples: []tempopb.Sample{
					{TimestampMs: 0, Value: 7},      // [2, 4, 6, 8, 10, 12, 14] - total: 7
					{TimestampMs: 15_000, Value: 7}, // [16, 18, 20, 22, 24, 26, 28] - total: 7
					{TimestampMs: 30_000, Value: 8}, // [30, 32, 34, 36, 38, 40, 42, 44] - total: 8
					{TimestampMs: 45_000, Value: 2}, // [46, 48]
					{TimestampMs: 60_000, Value: 0},
				},
			},
			{
				PromLabels: `{.service.name="odd"}`,
				Labels:     []common_v1.KeyValue{tempopb.MakeKeyValueString(".service.name", "odd")},
				Samples: []tempopb.Sample{
					{TimestampMs: 0, Value: 7},      // [1, 3, 5, 7, 9, 11, 13] - total: 7
					{TimestampMs: 15_000, Value: 8}, // [15, 17, 19, 21, 23, 25, 27, 29] - total: 8
					{TimestampMs: 30_000, Value: 7}, // [31, 33, 35, 37, 39, 41, 43] - total: 7
					{TimestampMs: 45_000, Value: 3}, // [45, 47, 49] - total: 3
					{TimestampMs: 60_000, Value: 0},
				},
			},
		},
	},
	{
		name: "min_over_time",
		req:  requestWithDefaultRange("{ } | min_over_time(duration)"),
		expected: []*tempopb.TimeSeries{
			{
				PromLabels: `{__name__="min_over_time"}`,
				Labels:     []common_v1.KeyValue{tempopb.MakeKeyValueString("__name__", "min_over_time")},
				Samples: []tempopb.Sample{
					{TimestampMs: 0, Value: 1},       // Interval [1, 14], min is 1
					{TimestampMs: 15_000, Value: 15}, // Interval [15, 29], min is 15
					{TimestampMs: 30_000, Value: 30}, // Interval [30, 44], min is 30
					{TimestampMs: 45_000, Value: 45}, // Interval [45, 50), min is 45
				},
			},
		},
	},
	{
		name: "max_over_time",
		req:  requestWithDefaultRange("{ } | max_over_time(duration)"),
		expected: []*tempopb.TimeSeries{
			{
				PromLabels: `{__name__="max_over_time"}`,
				Labels:     []common_v1.KeyValue{tempopb.MakeKeyValueString("__name__", "max_over_time")},
				Samples: []tempopb.Sample{
					{TimestampMs: 0, Value: 14},      // Interval [1, 14], max is 14
					{TimestampMs: 15_000, Value: 29}, // Interval [15, 29], max is 29
					{TimestampMs: 30_000, Value: 44}, // Interval [30, 44], max is 44
					{TimestampMs: 45_000, Value: 49}, // Interval [45, 50), max is 49
				},
			},
		},
	},
	{
		name: "avg_over_time",
		req:  requestWithDefaultRange("{ } | avg_over_time(duration)"),
		expected: []*tempopb.TimeSeries{
			{
				PromLabels: `{__name__="avg_over_time"}`,
				Labels:     []common_v1.KeyValue{tempopb.MakeKeyValueString("__name__", "avg_over_time")},
				Samples: []tempopb.Sample{
					{TimestampMs: 0, Value: 105 / 14.0},      // sum from 1 to 14 is 105
					{TimestampMs: 15_000, Value: 330 / 15.0}, // sum from 15 to 29 is 330
					{TimestampMs: 30_000, Value: 555 / 15.0}, // sum from 30 to 44 is 555
					{TimestampMs: 45_000, Value: 235 / 5.0},  // sum from 45 to 49 is 235
				},
			},
			{
				PromLabels: `{__meta_type="__count", __name__="avg_over_time"}`,
				Labels: []common_v1.KeyValue{
					tempopb.MakeKeyValueString("__name__", "avg_over_time"),
					tempopb.MakeKeyValueString("__meta_type", "__count"),
				},
				Samples: []tempopb.Sample{
					{TimestampMs: 0, Value: 14},
					{TimestampMs: 15_000, Value: 15},
					{TimestampMs: 30_000, Value: 15},
					{TimestampMs: 45_000, Value: 5},
				},
			},
		},
	},
	{
		name: "sum_over_time",
		req:  requestWithDefaultRange("{ } | sum_over_time(duration)"),
		expected: []*tempopb.TimeSeries{
			{
				PromLabels: `{__name__="sum_over_time"}`,
				Labels:     []common_v1.KeyValue{tempopb.MakeKeyValueString("__name__", "sum_over_time")},
				Samples: []tempopb.Sample{
					{TimestampMs: 0, Value: 105},      // sum from 1 to 14 is 105
					{TimestampMs: 15_000, Value: 330}, // sum from 15 to 29 is 330
					{TimestampMs: 30_000, Value: 555}, // sum from 30 to 44 is 555
					{TimestampMs: 45_000, Value: 235}, // sum from 45 to 49 is 235
				},
			},
		},
	},
	{
		name: "quantile_over_time",
		req:  requestWithDefaultRange("{ } | quantile_over_time(duration, .5)"),
		expected: []*tempopb.TimeSeries{
			{
				PromLabels: `{__bucket="1.073741824"}`,
				Labels:     []common_v1.KeyValue{tempopb.MakeKeyValueDouble("__bucket", 1.073741824)},
				Samples: []tempopb.Sample{
					{TimestampMs: 0, Value: 1}, // 1 number (1) is less than 1.07
					{TimestampMs: 15_000, Value: 0},
					{TimestampMs: 30_000, Value: 0},
					{TimestampMs: 45_000, Value: 0},
					{TimestampMs: 60_000, Value: 0},
				},
			},
			{
				PromLabels: `{__bucket="2.147483648"}`,
				Labels:     []common_v1.KeyValue{tempopb.MakeKeyValueDouble("__bucket", 2.147483648)},
				Samples: []tempopb.Sample{
					{TimestampMs: 0, Value: 1}, // 1 number (2) is between 1.07 and 2.15
					{TimestampMs: 15_000, Value: 0},
					{TimestampMs: 30_000, Value: 0},
					{TimestampMs: 45_000, Value: 0},
					{TimestampMs: 60_000, Value: 0},
				},
			},
			{
				PromLabels: `{__bucket="4.294967296"}`,
				Labels:     []common_v1.KeyValue{tempopb.MakeKeyValueDouble("__bucket", 4.294967296)},
				Samples: []tempopb.Sample{
					{TimestampMs: 0, Value: 2}, // 2 numbers (3, 4) are between 2.15 and 4.29
					{TimestampMs: 15_000, Value: 0},
					{TimestampMs: 30_000, Value: 0},
					{TimestampMs: 45_000, Value: 0},
					{TimestampMs: 60_000, Value: 0},
				},
			},
			{
				PromLabels: `{__bucket="8.589934592"}`,
				Labels:     []common_v1.KeyValue{tempopb.MakeKeyValueDouble("__bucket", 8.589934592)},
				Samples: []tempopb.Sample{
					{TimestampMs: 0, Value: 4}, // 5, 6, 7, 8
					{TimestampMs: 15_000, Value: 0},
					{TimestampMs: 30_000, Value: 0},
					{TimestampMs: 45_000, Value: 0},
					{TimestampMs: 60_000, Value: 0},
				},
			},
			{
				PromLabels: `{__bucket="17.179869184"}`,
				Labels:     []common_v1.KeyValue{tempopb.MakeKeyValueDouble("__bucket", 17.179869184)},
				Samples: []tempopb.Sample{
					{TimestampMs: 0, Value: 6}, // 9, 10, 11, 12, 13, 14
					{TimestampMs: 15_000, Value: 3},
					{TimestampMs: 30_000, Value: 0},
					{TimestampMs: 45_000, Value: 0},
					{TimestampMs: 60_000, Value: 0},
				},
			},
			{
				PromLabels: `{__bucket="34.359738368"}`,
				Labels:     []common_v1.KeyValue{tempopb.MakeKeyValueDouble("__bucket", 34.359738368)},
				Samples: []tempopb.Sample{
					{TimestampMs: 0, Value: 0},
					{TimestampMs: 15_000, Value: 12}, // and so on
					{TimestampMs: 30_000, Value: 5},
					{TimestampMs: 45_000, Value: 0},
					{TimestampMs: 60_000, Value: 0},
				},
			},
			{
				PromLabels: `{__bucket="68.719476736"}`,
				Labels:     []common_v1.KeyValue{tempopb.MakeKeyValueDouble("__bucket", 68.719476736)},
				Samples: []tempopb.Sample{
					{TimestampMs: 0, Value: 0},
					{TimestampMs: 15_000, Value: 0},
					{TimestampMs: 30_000, Value: 10},
					{TimestampMs: 45_000, Value: 5},
					{TimestampMs: 60_000, Value: 0},
				},
			},
		},
	},
	{
		name: "histogram_over_time",
		req:  requestWithDefaultRange("{ } | histogram_over_time(duration)"),
		expected: []*tempopb.TimeSeries{
			{
				PromLabels: `{__bucket="1.073741824"}`,
				Labels:     []common_v1.KeyValue{tempopb.MakeKeyValueDouble("__bucket", 1.073741824)},
				Samples: []tempopb.Sample{
					{TimestampMs: 0, Value: 1},
					{TimestampMs: 15_000, Value: 0},
					{TimestampMs: 30_000, Value: 0},
					{TimestampMs: 45_000, Value: 0},
					{TimestampMs: 60_000, Value: 0},
				},
			},
			{
				PromLabels: `{__bucket="2.147483648"}`,
				Labels:     []common_v1.KeyValue{tempopb.MakeKeyValueDouble("__bucket", 2.147483648)},
				Samples: []tempopb.Sample{
					{TimestampMs: 0, Value: 1},
					{TimestampMs: 15_000, Value: 0},
					{TimestampMs: 30_000, Value: 0},
					{TimestampMs: 45_000, Value: 0},
					{TimestampMs: 60_000, Value: 0},
				},
			},
			{
				PromLabels: `{__bucket="4.294967296"}`,
				Labels:     []common_v1.KeyValue{tempopb.MakeKeyValueDouble("__bucket", 4.294967296)},
				Samples: []tempopb.Sample{
					{TimestampMs: 0, Value: 2},
					{TimestampMs: 15_000, Value: 0},
					{TimestampMs: 30_000, Value: 0},
					{TimestampMs: 45_000, Value: 0},
					{TimestampMs: 60_000, Value: 0},
				},
			},
			{
				PromLabels: `{__bucket="8.589934592"}`,
				Labels:     []common_v1.KeyValue{tempopb.MakeKeyValueDouble("__bucket", 8.589934592)},
				Samples: []tempopb.Sample{
					{TimestampMs: 0, Value: 4},
					{TimestampMs: 15_000, Value: 0},
					{TimestampMs: 30_000, Value: 0},
					{TimestampMs: 45_000, Value: 0},
					{TimestampMs: 60_000, Value: 0},
				},
			},
			{
				PromLabels: `{__bucket="17.179869184"}`,
				Labels:     []common_v1.KeyValue{tempopb.MakeKeyValueDouble("__bucket", 17.179869184)},
				Samples: []tempopb.Sample{
					{TimestampMs: 0, Value: 6},
					{TimestampMs: 15_000, Value: 3},
					{TimestampMs: 30_000, Value: 0},
					{TimestampMs: 45_000, Value: 0},
					{TimestampMs: 60_000, Value: 0},
				},
			},
			{
				PromLabels: `{__bucket="34.359738368"}`,
				Labels:     []common_v1.KeyValue{tempopb.MakeKeyValueDouble("__bucket", 34.359738368)},
				Samples: []tempopb.Sample{
					{TimestampMs: 0, Value: 0},
					{TimestampMs: 15_000, Value: 12},
					{TimestampMs: 30_000, Value: 5},
					{TimestampMs: 45_000, Value: 0},
					{TimestampMs: 60_000, Value: 0},
				},
			},
			{
				PromLabels: `{__bucket="68.719476736"}`,
				Labels:     []common_v1.KeyValue{tempopb.MakeKeyValueDouble("__bucket", 68.719476736)},
				Samples: []tempopb.Sample{
					{TimestampMs: 0, Value: 0},
					{TimestampMs: 15_000, Value: 0},
					{TimestampMs: 30_000, Value: 10},
					{TimestampMs: 45_000, Value: 5},
					{TimestampMs: 60_000, Value: 0},
				},
			},
		},
	},
	// --- Non-standard range queries ---
	{
		name: "end<step",
		req: &tempopb.QueryRangeRequest{
			Start: 1,
			End:   3 * uint64(time.Second),
			Step:  5 * uint64(time.Second),
			Query: `{ } | count_over_time()`,
		},
		expected: []*tempopb.TimeSeries{
			{
				PromLabels: `{__name__="count_over_time"}`,
				Labels:     []common_v1.KeyValue{tempopb.MakeKeyValueString("__name__", "count_over_time")},
				Samples: []tempopb.Sample{
					{TimestampMs: 0, Value: 2}, // 1, 2
					{TimestampMs: 5_000, Value: 0},
				},
			},
		},
	},
	{
		name: "end=step",
		req: &tempopb.QueryRangeRequest{
			Start: 1,
			End:   5 * uint64(time.Second),
			Step:  5 * uint64(time.Second),
			Query: `{ } | count_over_time()`,
		},
		expected: []*tempopb.TimeSeries{
			{
				PromLabels: `{__name__="count_over_time"}`,
				Labels:     []common_v1.KeyValue{tempopb.MakeKeyValueString("__name__", "count_over_time")},
				Samples: []tempopb.Sample{
					{TimestampMs: 0, Value: 4}, // 1, 2, 3, 4
					{TimestampMs: 5_000, Value: 0},
				},
			},
		},
	},
	{
		name: "small step",
		req: &tempopb.QueryRangeRequest{
			Start: 1,
			End:   3 * uint64(time.Second),
			Step:  500 * uint64(time.Millisecond),
			Query: `{ } | count_over_time()`,
		},
		expected: []*tempopb.TimeSeries{
			{
				PromLabels: `{__name__="count_over_time"}`,
				Labels:     []common_v1.KeyValue{tempopb.MakeKeyValueString("__name__", "count_over_time")},
				Samples: []tempopb.Sample{
					{TimestampMs: 0, Value: 0},
					{TimestampMs: 500, Value: 0},
					{TimestampMs: 1000, Value: 1},
					{TimestampMs: 1500, Value: 0},
					{TimestampMs: 2000, Value: 1},
					{TimestampMs: 2500, Value: 0},
					{TimestampMs: 3000, Value: 0},
				},
			},
		},
	},
}

func TestTempoDBQueryRange(t *testing.T) {
	var (
		tempDir      = t.TempDir()
		blockVersion = vparquet4.VersionString
	)

	dc := backend.DedicatedColumns{
		{Scope: "resource", Name: "res-dedicated.01", Type: "string"},
		{Scope: "resource", Name: "res-dedicated.02", Type: "string"},
		{Scope: "span", Name: "span-dedicated.01", Type: "string"},
		{Scope: "span", Name: "span-dedicated.02", Type: "string"},
	}
	r, w, c, err := New(&Config{
		Backend: backend.Local,
		Local: &local.Config{
			Path: path.Join(tempDir, "traces"),
		},
		Block: &common.BlockConfig{
			IndexDownsampleBytes: 17,
			BloomFP:              .01,
			BloomShardSizeBytes:  100_000,
			Version:              blockVersion,
			IndexPageSizeBytes:   1000,
			RowGroupSizeBytes:    10000,
			DedicatedColumns:     dc,
		},
		WAL: &wal.Config{
			Filepath:       path.Join(tempDir, "wal"),
			IngestionSlack: time.Since(time.Time{}),
		},
		Search: &SearchConfig{
			ChunkSizeBytes:  1_000_000,
			ReadBufferCount: 8, ReadBufferSizeBytes: 4 * 1024 * 1024,
		},
		BlocklistPoll: 0,
	}, nil, log.NewNopLogger())
	require.NoError(t, err)

	err = c.EnableCompaction(context.Background(), &CompactorConfig{
		ChunkSizeBytes:          10,
		MaxCompactionRange:      time.Hour,
		BlockRetention:          0,
		CompactedBlockRetention: 0,
	}, &mockSharder{}, &mockOverrides{})
	require.NoError(t, err)

	ctx := context.Background()
	r.EnablePolling(ctx, &mockJobSharder{})

	// Write to wal
	wal := w.WAL()

	meta := &backend.BlockMeta{BlockID: backend.NewUUID(), TenantID: testTenantID, DedicatedColumns: dc}
	head, err := wal.NewBlock(meta, model.CurrentEncoding)
	require.NoError(t, err)
	dec := model.MustNewSegmentDecoder(model.CurrentEncoding)

	totalSpans := 100
	for i := 1; i <= totalSpans; i++ {
		tid := test.ValidTraceID(nil)

		sp := test.MakeSpan(tid)

		// Start time is i seconds
		sp.StartTimeUnixNano = uint64(i * int(time.Second))

		// Duration is i seconds
		sp.EndTimeUnixNano = sp.StartTimeUnixNano + uint64(i*int(time.Second))

		// Service name
		var svcName string
		if i%2 == 0 {
			svcName = "even"
		} else {
			svcName = "odd"
		}

		tr := &tempopb.Trace{
			ResourceSpans: []*v1.ResourceSpans{
				{
					Resource: &resource_v1.Resource{
						Attributes: []*common_v1.KeyValue{tempopb.MakeKeyValueStringPtr("service.name", svcName)},
					},
					ScopeSpans: []*v1.ScopeSpans{
						{
							Spans: []*v1.Span{
								sp,
							},
						},
					},
				},
			},
		}

		b1, err := dec.PrepareForWrite(tr, 0, 0)
		require.NoError(t, err)

		b2, err := dec.ToObject([][]byte{b1})
		require.NoError(t, err)
		err = head.Append(tid, b2, 0, 0, true)
		require.NoError(t, err)
	}

	// Complete block
	block, err := w.CompleteBlock(context.Background(), head)
	require.NoError(t, err)

	f := traceql.NewSpansetFetcherWrapper(func(ctx context.Context, req traceql.FetchSpansRequest) (traceql.FetchSpansResponse, error) {
		return block.Fetch(ctx, req, common.DefaultSearchOptions())
	})

	for _, tc := range queryRangeTestCases {
		t.Run(tc.name, func(t *testing.T) {
			eval, err := traceql.NewEngine().CompileMetricsQueryRange(tc.req, 0, 0, false)
			require.NoError(t, err)

			err = eval.Do(ctx, f, 0, 0)
			require.NoError(t, err)

			actual := eval.Results().ToProto(tc.req)

			// Slice order is not deterministic, so we sort the slices before comparing
			sort.Slice(tc.expected, func(i, j int) bool {
				return tc.expected[i].PromLabels < tc.expected[j].PromLabels
			})
			sort.Slice(actual, func(i, j int) bool {
				return actual[i].PromLabels < actual[j].PromLabels
			})

			if diff := cmp.Diff(tc.expected, actual, floatComparer); diff != "" {
				t.Errorf("Query: %v\n Diff: %v", tc.req.Query, diff)
			}
		})
	}
}

var floatComparer = cmp.Comparer(func(x, y float64) bool {
	return math.Abs(x-y) < 1e-6
})

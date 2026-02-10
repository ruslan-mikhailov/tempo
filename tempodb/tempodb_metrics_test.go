package tempodb

import (
	"context"
	"fmt"
	"math"
	"os"
	"path"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
	"github.com/grafana/tempo/pkg/model"
	"github.com/grafana/tempo/pkg/tempopb"
	common_v1 "github.com/grafana/tempo/pkg/tempopb/common/v1"
	resource_v1 "github.com/grafana/tempo/pkg/tempopb/resource/v1"
	v1 "github.com/grafana/tempo/pkg/tempopb/trace/v1"
	"github.com/grafana/tempo/pkg/traceql"
	"github.com/grafana/tempo/pkg/util/test"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/backend/local"
	"github.com/grafana/tempo/tempodb/encoding"
	"github.com/grafana/tempo/tempodb/encoding/common"
	"github.com/grafana/tempo/tempodb/encoding/vparquet4"
	"github.com/grafana/tempo/tempodb/encoding/vparquet5"
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
	name string
	req  *tempopb.QueryRangeRequest
	// expectedL1 is the expected result of the first level of aggregation
	expectedL1 []*tempopb.TimeSeries
	// expectedL2 is the expected result of the second level of aggregation
	// if nil, the data is not changed and expected results are from previous step
	expectedL2 []*tempopb.TimeSeries
	// expectedL3 is the expected result of the third level of aggregation
	// if nil, the data is not changed and expected results are from previous step
	expectedL3 []*tempopb.TimeSeries
}{
	{
		name: "rate",
		req:  requestWithDefaultRange("{ } | rate()"),
		expectedL1: []*tempopb.TimeSeries{
			{
				Labels: []common_v1.KeyValue{tempopb.MakeKeyValueString("__name__", "rate")},
				Samples: []tempopb.Sample{
					{TimestampMs: 15_000, Value: 1.0},        // Interval (0, 15], 15 spans at 1-15
					{TimestampMs: 30_000, Value: 1.0},        // Interval (15, 30], 15 spans
					{TimestampMs: 45_000, Value: 1.0},        // Interval (30, 45], 15 spans
					{TimestampMs: 60_000, Value: 5.0 / 15.0}, // Interval (45, 50], 5 spans
				},
			},
		},
		expectedL2: []*tempopb.TimeSeries{
			{
				Labels: []common_v1.KeyValue{tempopb.MakeKeyValueString("__name__", "rate")},
				// with two sources rate will be doubled
				Samples: []tempopb.Sample{
					{TimestampMs: 15_000, Value: 2 * 15.0 / 15.0},
					{TimestampMs: 30_000, Value: 2 * 1.0},
					{TimestampMs: 45_000, Value: 2 * 1.0},
					{TimestampMs: 60_000, Value: 2 * 5.0 / 15.0},
				},
			},
		},
	},
	{
		name: "rate_with_filter",
		req:  requestWithDefaultRange(`{ .service.name="even" } | rate()`),
		expectedL1: []*tempopb.TimeSeries{
			{
				Labels: []common_v1.KeyValue{tempopb.MakeKeyValueString("__name__", "rate")},
				Samples: []tempopb.Sample{
					{TimestampMs: 15_000, Value: 7.0 / 15.0}, // Interval (0, 15], 7 spans at 2, 4, 6, 8, 10, 12, 14
					{TimestampMs: 30_000, Value: 8.0 / 15.0}, // Interval (15, 30], 8 spans at 16, 18, 20, 22, 24, 26, 28, 30
					{TimestampMs: 45_000, Value: 7.0 / 15.0}, // Interval (30, 45], 7 spans at 32, 34, 36, 38, 40, 42, 44
					{TimestampMs: 60_000, Value: 3.0 / 15.0}, // Interval (45, 50], 3 spans at 46, 48, 50
				},
			},
		},
		expectedL2: []*tempopb.TimeSeries{
			{
				Labels: []common_v1.KeyValue{tempopb.MakeKeyValueString("__name__", "rate")},
				// with two sources rate will be doubled
				Samples: []tempopb.Sample{
					{TimestampMs: 15_000, Value: 2 * 7.0 / 15.0},
					{TimestampMs: 30_000, Value: 2 * 8.0 / 15.0},
					{TimestampMs: 45_000, Value: 2 * 7.0 / 15.0},
					{TimestampMs: 60_000, Value: 2 * 3.0 / 15.0},
				},
			},
		},
	},
	{
		name: "rate_no_spans",
		req:  requestWithDefaultRange(`{ .service.name="does_not_exist" } | rate()`),
		expectedL1: []*tempopb.TimeSeries{
			{
				Labels: []common_v1.KeyValue{tempopb.MakeKeyValueString("__name__", "rate")},
				Samples: []tempopb.Sample{
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
		expectedL1: []*tempopb.TimeSeries{
			{
				Labels: []common_v1.KeyValue{tempopb.MakeKeyValueString("__name__", "count_over_time")},
				Samples: []tempopb.Sample{
					{TimestampMs: 15_000, Value: 15}, // Interval (0, 15], 15 spans
					{TimestampMs: 30_000, Value: 15}, // Interval (15, 30], 15 spans
					{TimestampMs: 45_000, Value: 15}, // Interval (30, 45], 15 spans
					{TimestampMs: 60_000, Value: 5},  // Interval (45, 50], 5 spans
				},
			},
		},
		expectedL2: []*tempopb.TimeSeries{
			{
				Labels: []common_v1.KeyValue{tempopb.MakeKeyValueString("__name__", "count_over_time")},
				// with two sources count will be doubled
				Samples: []tempopb.Sample{
					{TimestampMs: 15_000, Value: 2 * 15},
					{TimestampMs: 30_000, Value: 2 * 15},
					{TimestampMs: 45_000, Value: 2 * 15},
					{TimestampMs: 60_000, Value: 2 * 5},
				},
			},
		},
	},
	{
		name: "count_over_time",
		req:  requestWithDefaultRange(`{ } | count_over_time() by (.service.name)`),
		expectedL1: []*tempopb.TimeSeries{
			{
				Labels: []common_v1.KeyValue{tempopb.MakeKeyValueString(".service.name", "even")},
				Samples: []tempopb.Sample{
					{TimestampMs: 15_000, Value: 7}, // (0, 15]: [2, 4, 6, 8, 10, 12, 14] - total: 7
					{TimestampMs: 30_000, Value: 8}, // (15, 30]: [16, 18, 20, 22, 24, 26, 28, 30] - total: 8
					{TimestampMs: 45_000, Value: 7}, // (30, 45]: [32, 34, 36, 38, 40, 42, 44] - total: 7 (30 is now excluded)
					{TimestampMs: 60_000, Value: 3}, // (45, 50]: [46, 48, 50] - total: 3
				},
			},
			{
				Labels: []common_v1.KeyValue{tempopb.MakeKeyValueString(".service.name", "odd")},
				Samples: []tempopb.Sample{
					{TimestampMs: 15_000, Value: 8}, // (0, 15]: [1, 3, 5, 7, 9, 11, 13, 15] - total: 8
					{TimestampMs: 30_000, Value: 7}, // (15, 30]: [17, 19, 21, 23, 25, 27, 29] - total: 7
					{TimestampMs: 45_000, Value: 8}, // (30, 45]: [31, 33, 35, 37, 39, 41, 43, 45] - total: 8
					{TimestampMs: 60_000, Value: 2}, // (45, 50]: [47, 49] - total: 2
				},
			},
		},
		expectedL2: []*tempopb.TimeSeries{
			{
				Labels: []common_v1.KeyValue{tempopb.MakeKeyValueString(".service.name", "even")},
				Samples: []tempopb.Sample{
					{TimestampMs: 15_000, Value: 2 * 7},
					{TimestampMs: 30_000, Value: 2 * 8},
					{TimestampMs: 45_000, Value: 2 * 7},
					{TimestampMs: 60_000, Value: 2 * 3},
				},
			},
			{
				Labels: []common_v1.KeyValue{tempopb.MakeKeyValueString(".service.name", "odd")},
				Samples: []tempopb.Sample{
					{TimestampMs: 15_000, Value: 2 * 8},
					{TimestampMs: 30_000, Value: 2 * 7},
					{TimestampMs: 45_000, Value: 2 * 8},
					{TimestampMs: 60_000, Value: 2 * 2},
				},
			},
		},
	},
	{
		name: "min_over_time",
		req:  requestWithDefaultRange("{ } | min_over_time(duration)"),
		expectedL1: []*tempopb.TimeSeries{
			{
				Labels: []common_v1.KeyValue{tempopb.MakeKeyValueString("__name__", "min_over_time")},
				Samples: []tempopb.Sample{
					{TimestampMs: 15_000, Value: 1},  // Interval (0, 15], min is 1
					{TimestampMs: 30_000, Value: 16}, // Interval (15, 30], min is 16
					{TimestampMs: 45_000, Value: 31}, // Interval (30, 45], min is 31
					{TimestampMs: 60_000, Value: 46}, // Interval (45, 50], min is 46
				},
			},
		},
		expectedL2: nil, // results should be the same: min(a, a) = a
	},
	{
		name: "max_over_time",
		req:  requestWithDefaultRange("{ } | max_over_time(duration)"),
		expectedL1: []*tempopb.TimeSeries{
			{
				Labels: []common_v1.KeyValue{tempopb.MakeKeyValueString("__name__", "max_over_time")},
				Samples: []tempopb.Sample{
					{TimestampMs: 15_000, Value: 15}, // Interval (0, 15], max is 15
					{TimestampMs: 30_000, Value: 30}, // Interval (15, 30], max is 30
					{TimestampMs: 45_000, Value: 45}, // Interval (30, 45], max is 45
					{TimestampMs: 60_000, Value: 50}, // Interval (45, 50], max is 50
				},
			},
		},
		expectedL2: nil, // results should be the same: max(a, a) = a
	},
	{
		name: "avg_over_time",
		req:  requestWithDefaultRange("{ } | avg_over_time(duration)"),
		expectedL1: []*tempopb.TimeSeries{
			{
				Labels: []common_v1.KeyValue{tempopb.MakeKeyValueString("__name__", "avg_over_time")},
				Samples: []tempopb.Sample{
					{TimestampMs: 15_000, Value: 120 / 15.0}, // sum from 1 to 15 is 120
					{TimestampMs: 30_000, Value: 345 / 15.0}, // sum from 16 to 30 is 345
					{TimestampMs: 45_000, Value: 570 / 15.0}, // sum from 31 to 45 is 570
					{TimestampMs: 60_000, Value: 240 / 5.0},  // sum from 46 to 50 is 240
				},
			},
			{
				Labels: []common_v1.KeyValue{
					tempopb.MakeKeyValueString("__name__", "avg_over_time"),
					tempopb.MakeKeyValueString("__meta_type", "__count"),
				},
				Samples: []tempopb.Sample{
					{TimestampMs: 15_000, Value: 15},
					{TimestampMs: 30_000, Value: 15},
					{TimestampMs: 45_000, Value: 15},
					{TimestampMs: 60_000, Value: 5},
				},
			},
		},
		expectedL2: []*tempopb.TimeSeries{
			{
				Labels: []common_v1.KeyValue{tempopb.MakeKeyValueString("__name__", "avg_over_time")},
				Samples: []tempopb.Sample{
					{TimestampMs: 15_000, Value: 120 / 15.0}, // sum from 1 to 15 is 120
					{TimestampMs: 30_000, Value: 345 / 15.0}, // sum from 16 to 30 is 345
					{TimestampMs: 45_000, Value: 570 / 15.0}, // sum from 31 to 45 is 570
					{TimestampMs: 60_000, Value: 240 / 5.0},  // sum from 46 to 50 is 240
				},
			},
			{
				Labels: []common_v1.KeyValue{
					tempopb.MakeKeyValueString("__name__", "avg_over_time"),
					tempopb.MakeKeyValueString("__meta_type", "__count"),
				},
				Samples: []tempopb.Sample{
					{TimestampMs: 15_000, Value: 2 * 15},
					{TimestampMs: 30_000, Value: 2 * 15},
					{TimestampMs: 45_000, Value: 2 * 15},
					{TimestampMs: 60_000, Value: 2 * 5},
				},
			},
		},
		expectedL3: []*tempopb.TimeSeries{
			{
				Labels: []common_v1.KeyValue{tempopb.MakeKeyValueString("__name__", "avg_over_time")},
				Samples: []tempopb.Sample{
					{TimestampMs: 15_000, Value: 120 / 15.0},
					{TimestampMs: 30_000, Value: 345 / 15.0},
					{TimestampMs: 45_000, Value: 570 / 15.0},
					{TimestampMs: 60_000, Value: 240 / 5.0},
				},
			},
		},
	},
	{
		name: "sum_over_time",
		req:  requestWithDefaultRange("{ } | sum_over_time(duration)"),
		expectedL1: []*tempopb.TimeSeries{
			{
				Labels: []common_v1.KeyValue{tempopb.MakeKeyValueString("__name__", "sum_over_time")},
				Samples: []tempopb.Sample{
					{TimestampMs: 15_000, Value: 120}, // sum from 1 to 15 is 120
					{TimestampMs: 30_000, Value: 345}, // sum from 16 to 30 is 345 (including 30)
					{TimestampMs: 45_000, Value: 570}, // sum from 31 to 45 is 570
					{TimestampMs: 60_000, Value: 240}, // sum from 46 to 50 is 240
				},
			},
		},
		expectedL2: []*tempopb.TimeSeries{
			{
				Labels: []common_v1.KeyValue{tempopb.MakeKeyValueString("__name__", "sum_over_time")},
				Samples: []tempopb.Sample{
					{TimestampMs: 15_000, Value: 2 * 120},
					{TimestampMs: 30_000, Value: 2 * 345},
					{TimestampMs: 45_000, Value: 2 * 570},
					{TimestampMs: 60_000, Value: 2 * 240},
				},
			},
		},
	},
	{
		name: "quantile_over_time",
		req:  requestWithDefaultRange("{ } | quantile_over_time(duration, .5)"),
		// first two levels for quantile are buckets and count, then on level 3 we can compute the quantile
		expectedL1: []*tempopb.TimeSeries{
			{
				Labels: []common_v1.KeyValue{tempopb.MakeKeyValueDouble("__bucket", 1.073741824)},
				Samples: []tempopb.Sample{
					{TimestampMs: 15_000, Value: 1}, // 1 number (1) is less than 1.07
					{TimestampMs: 30_000, Value: 0},
					{TimestampMs: 45_000, Value: 0},
					{TimestampMs: 60_000, Value: 0},
				},
			},
			{
				Labels: []common_v1.KeyValue{tempopb.MakeKeyValueDouble("__bucket", 2.147483648)},
				Samples: []tempopb.Sample{
					{TimestampMs: 15_000, Value: 1}, // 1 number (2) is between 1.07 and 2.15
					{TimestampMs: 30_000, Value: 0},
					{TimestampMs: 45_000, Value: 0},
					{TimestampMs: 60_000, Value: 0},
				},
			},
			{
				Labels: []common_v1.KeyValue{tempopb.MakeKeyValueDouble("__bucket", 4.294967296)},
				Samples: []tempopb.Sample{
					{TimestampMs: 15_000, Value: 2}, // 2 numbers (3, 4) are between 2.15 and 4.29
					{TimestampMs: 30_000, Value: 0},
					{TimestampMs: 45_000, Value: 0},
					{TimestampMs: 60_000, Value: 0},
				},
			},
			{
				Labels: []common_v1.KeyValue{tempopb.MakeKeyValueDouble("__bucket", 8.589934592)},
				Samples: []tempopb.Sample{
					{TimestampMs: 15_000, Value: 4}, // 5, 6, 7, 8
					{TimestampMs: 30_000, Value: 0},
					{TimestampMs: 45_000, Value: 0},
					{TimestampMs: 60_000, Value: 0},
				},
			},
			{
				Labels: []common_v1.KeyValue{tempopb.MakeKeyValueDouble("__bucket", 17.179869184)},
				Samples: []tempopb.Sample{
					{TimestampMs: 15_000, Value: 7}, // 9, 10, 11, 12, 13, 14, 15 from interval (0,15]
					{TimestampMs: 30_000, Value: 2}, // 16, 17 from interval (15,30]
					{TimestampMs: 45_000, Value: 0},
					{TimestampMs: 60_000, Value: 0},
				},
			},
			{
				Labels: []common_v1.KeyValue{tempopb.MakeKeyValueDouble("__bucket", 34.359738368)},
				Samples: []tempopb.Sample{
					{TimestampMs: 15_000, Value: 0},
					{TimestampMs: 30_000, Value: 13}, // 18,19,20,21,22,23,24,25,26,27,28,29,30 from interval (15,30]
					{TimestampMs: 45_000, Value: 4},  // 31, 32, 33, 34 from interval (30,45]
					{TimestampMs: 60_000, Value: 0},
				},
			},
			{
				Labels: []common_v1.KeyValue{tempopb.MakeKeyValueDouble("__bucket", 68.719476736)},
				Samples: []tempopb.Sample{
					{TimestampMs: 15_000, Value: 0},
					{TimestampMs: 30_000, Value: 0},
					{TimestampMs: 45_000, Value: 11}, // 35,36,37,38,39,40,41,42,43,44,45 from interval (30,45]
					{TimestampMs: 60_000, Value: 5},  // 46,47,48,49,50 from interval (45,50]
				},
			},
		},
		expectedL2: []*tempopb.TimeSeries{
			{
				Labels: []common_v1.KeyValue{tempopb.MakeKeyValueDouble("__bucket", 1.073741824)},
				Samples: []tempopb.Sample{
					{TimestampMs: 15_000, Value: 2 * 1},
					{TimestampMs: 30_000, Value: 0},
					{TimestampMs: 45_000, Value: 0},
					{TimestampMs: 60_000, Value: 0},
				},
			},
			{
				Labels: []common_v1.KeyValue{tempopb.MakeKeyValueDouble("__bucket", 2.147483648)},
				Samples: []tempopb.Sample{
					{TimestampMs: 15_000, Value: 2 * 1},
					{TimestampMs: 30_000, Value: 0},
					{TimestampMs: 45_000, Value: 0},
					{TimestampMs: 60_000, Value: 0},
				},
			},
			{
				Labels: []common_v1.KeyValue{tempopb.MakeKeyValueDouble("__bucket", 4.294967296)},
				Samples: []tempopb.Sample{
					{TimestampMs: 15_000, Value: 2 * 2},
					{TimestampMs: 30_000, Value: 0},
					{TimestampMs: 45_000, Value: 0},
					{TimestampMs: 60_000, Value: 0},
				},
			},
			{
				Labels: []common_v1.KeyValue{tempopb.MakeKeyValueDouble("__bucket", 8.589934592)},
				Samples: []tempopb.Sample{
					{TimestampMs: 15_000, Value: 2 * 4},
					{TimestampMs: 30_000, Value: 0},
					{TimestampMs: 45_000, Value: 0},
					{TimestampMs: 60_000, Value: 0},
				},
			},
			{
				Labels: []common_v1.KeyValue{tempopb.MakeKeyValueDouble("__bucket", 17.179869184)},
				Samples: []tempopb.Sample{
					{TimestampMs: 15_000, Value: 2 * 7},
					{TimestampMs: 30_000, Value: 2 * 2},
					{TimestampMs: 45_000, Value: 0},
					{TimestampMs: 60_000, Value: 0},
				},
			},
			{
				Labels: []common_v1.KeyValue{tempopb.MakeKeyValueDouble("__bucket", 34.359738368)},
				Samples: []tempopb.Sample{
					{TimestampMs: 15_000, Value: 0},
					{TimestampMs: 30_000, Value: 2 * 13},
					{TimestampMs: 45_000, Value: 2 * 4},
					{TimestampMs: 60_000, Value: 0},
				},
			},
			{
				Labels: []common_v1.KeyValue{tempopb.MakeKeyValueDouble("__bucket", 68.719476736)},
				Samples: []tempopb.Sample{
					{TimestampMs: 15_000, Value: 0},
					{TimestampMs: 30_000, Value: 0},
					{TimestampMs: 45_000, Value: 2 * 11},
					{TimestampMs: 60_000, Value: 2 * 5},
				},
			},
		},
		expectedL3: []*tempopb.TimeSeries{
			{
				Labels: []common_v1.KeyValue{tempopb.MakeKeyValueDouble("p", 0.5)},
				Samples: []tempopb.Sample{
					// 1 2 3 4 5 6 7 || 8 || 9 10 11 12 13 14 15
					{TimestampMs: 15_000, Value: 7.877004751727669},
					// 16 17 18 19 20 21 22 || 23 || 24 25 26 27 28 29 30
					{TimestampMs: 30_000, Value: 23.03449508051292},
					// Actual q50 is 38. On low number of samples, the quantile can be inaccurate
					// 31 32 33 34 35 36 37 || 38 || 39 40 41 42 43 44 45
					{TimestampMs: 45_000, Value: 42.83828931515888},
					// Actual q50 is 48
					// 46 47 || 48 || 49 50
					{TimestampMs: 60_000, Value: 48.592007999616804},
				},
			},
		},
	},
	{
		name: "histogram_over_time",
		req:  requestWithDefaultRange("{ } | histogram_over_time(duration)"),
		expectedL1: []*tempopb.TimeSeries{
			{
				Labels: []common_v1.KeyValue{tempopb.MakeKeyValueDouble("__bucket", 1.073741824)},
				Samples: []tempopb.Sample{
					{TimestampMs: 15_000, Value: 1}, // 1 number (1) is less than 1.07
					{TimestampMs: 30_000, Value: 0},
					{TimestampMs: 45_000, Value: 0},
					{TimestampMs: 60_000, Value: 0},
				},
			},
			{
				Labels: []common_v1.KeyValue{tempopb.MakeKeyValueDouble("__bucket", 2.147483648)},
				Samples: []tempopb.Sample{
					{TimestampMs: 15_000, Value: 1}, // 1 number (2) is between 1.07 and 2.15
					{TimestampMs: 30_000, Value: 0},
					{TimestampMs: 45_000, Value: 0},
					{TimestampMs: 60_000, Value: 0},
				},
			},
			{
				Labels: []common_v1.KeyValue{tempopb.MakeKeyValueDouble("__bucket", 4.294967296)},
				Samples: []tempopb.Sample{
					{TimestampMs: 15_000, Value: 2}, // 2 numbers (3, 4) are between 2.15 and 4.29
					{TimestampMs: 30_000, Value: 0},
					{TimestampMs: 45_000, Value: 0},
					{TimestampMs: 60_000, Value: 0},
				},
			},
			{
				Labels: []common_v1.KeyValue{tempopb.MakeKeyValueDouble("__bucket", 8.589934592)},
				Samples: []tempopb.Sample{
					{TimestampMs: 15_000, Value: 4}, // 5, 6, 7, 8
					{TimestampMs: 30_000, Value: 0},
					{TimestampMs: 45_000, Value: 0},
					{TimestampMs: 60_000, Value: 0},
				},
			},
			{
				Labels: []common_v1.KeyValue{tempopb.MakeKeyValueDouble("__bucket", 17.179869184)},
				Samples: []tempopb.Sample{
					{TimestampMs: 15_000, Value: 7}, // 9, 10, 11, 12, 13, 14, 15 from interval (0,15]
					{TimestampMs: 30_000, Value: 2}, // 16, 17 from interval (15,30]
					{TimestampMs: 45_000, Value: 0},
					{TimestampMs: 60_000, Value: 0},
				},
			},
			{
				Labels: []common_v1.KeyValue{tempopb.MakeKeyValueDouble("__bucket", 34.359738368)},
				Samples: []tempopb.Sample{
					{TimestampMs: 15_000, Value: 0},
					{TimestampMs: 30_000, Value: 13}, // 18,19,20,21,22,23,24,25,26,27,28,29,30 from interval (15,30]
					{TimestampMs: 45_000, Value: 4},  // 31, 32, 33, 34 from interval (30,45]
					{TimestampMs: 60_000, Value: 0},
				},
			},
			{
				Labels: []common_v1.KeyValue{tempopb.MakeKeyValueDouble("__bucket", 68.719476736)},
				Samples: []tempopb.Sample{
					{TimestampMs: 15_000, Value: 0},
					{TimestampMs: 30_000, Value: 0},
					{TimestampMs: 45_000, Value: 11}, // 35,36,37,38,39,40,41,42,43,44,45 from interval (30,45]
					{TimestampMs: 60_000, Value: 5},  // 46,47,48,49,50 from interval (45,50]
				},
			},
		},
		expectedL2: []*tempopb.TimeSeries{
			{
				Labels: []common_v1.KeyValue{tempopb.MakeKeyValueDouble("__bucket", 1.073741824)},
				Samples: []tempopb.Sample{
					{TimestampMs: 15_000, Value: 2 * 1},
					{TimestampMs: 30_000, Value: 0},
					{TimestampMs: 45_000, Value: 0},
					{TimestampMs: 60_000, Value: 0},
				},
			},
			{
				Labels: []common_v1.KeyValue{tempopb.MakeKeyValueDouble("__bucket", 2.147483648)},
				Samples: []tempopb.Sample{
					{TimestampMs: 15_000, Value: 2 * 1},
					{TimestampMs: 30_000, Value: 0},
					{TimestampMs: 45_000, Value: 0},
					{TimestampMs: 60_000, Value: 0},
				},
			},
			{
				Labels: []common_v1.KeyValue{tempopb.MakeKeyValueDouble("__bucket", 4.294967296)},
				Samples: []tempopb.Sample{
					{TimestampMs: 15_000, Value: 2 * 2},
					{TimestampMs: 30_000, Value: 0},
					{TimestampMs: 45_000, Value: 0},
					{TimestampMs: 60_000, Value: 0},
				},
			},
			{
				Labels: []common_v1.KeyValue{tempopb.MakeKeyValueDouble("__bucket", 8.589934592)},
				Samples: []tempopb.Sample{
					{TimestampMs: 15_000, Value: 2 * 4},
					{TimestampMs: 30_000, Value: 0},
					{TimestampMs: 45_000, Value: 0},
					{TimestampMs: 60_000, Value: 0},
				},
			},
			{
				Labels: []common_v1.KeyValue{tempopb.MakeKeyValueDouble("__bucket", 17.179869184)},
				Samples: []tempopb.Sample{
					{TimestampMs: 15_000, Value: 2 * 7},
					{TimestampMs: 30_000, Value: 2 * 2},
					{TimestampMs: 45_000, Value: 0},
					{TimestampMs: 60_000, Value: 0},
				},
			},
			{
				Labels: []common_v1.KeyValue{tempopb.MakeKeyValueDouble("__bucket", 34.359738368)},
				Samples: []tempopb.Sample{
					{TimestampMs: 15_000, Value: 0},
					{TimestampMs: 30_000, Value: 2 * 13},
					{TimestampMs: 45_000, Value: 2 * 4},
					{TimestampMs: 60_000, Value: 0},
				},
			},
			{
				Labels: []common_v1.KeyValue{tempopb.MakeKeyValueDouble("__bucket", 68.719476736)},
				Samples: []tempopb.Sample{
					{TimestampMs: 15_000, Value: 0},
					{TimestampMs: 30_000, Value: 0},
					{TimestampMs: 45_000, Value: 2 * 11},
					{TimestampMs: 60_000, Value: 2 * 5},
				},
			},
		},
	},
	{
		name:       "compare_no_spans",
		req:        requestWithDefaultRange(`{ .service.name="does_not_exist" } | compare({ })`),
		expectedL1: []*tempopb.TimeSeries{},
	},
	{
		name:       "compare_no_spans",
		req:        requestWithDefaultRange(`{ .service.name="does_not_exist" } | compare({ .service.name="does_not_exist_for_sure" })`),
		expectedL1: []*tempopb.TimeSeries{},
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
		expectedL1: []*tempopb.TimeSeries{
			{
				Labels: []common_v1.KeyValue{tempopb.MakeKeyValueString("__name__", "count_over_time")},
				Samples: []tempopb.Sample{
					{TimestampMs: 5_000, Value: 3}, // 1, 2, 3
				},
			},
		},
		expectedL2: []*tempopb.TimeSeries{
			{
				Labels: []common_v1.KeyValue{tempopb.MakeKeyValueString("__name__", "count_over_time")},
				Samples: []tempopb.Sample{
					{TimestampMs: 5_000, Value: 3 * 2},
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
		expectedL1: []*tempopb.TimeSeries{
			{
				Labels: []common_v1.KeyValue{tempopb.MakeKeyValueString("__name__", "count_over_time")},
				Samples: []tempopb.Sample{
					{TimestampMs: 5_000, Value: 5}, // 1, 2, 3, 4, 5
				},
			},
		},
		expectedL2: []*tempopb.TimeSeries{
			{
				Labels: []common_v1.KeyValue{tempopb.MakeKeyValueString("__name__", "count_over_time")},
				Samples: []tempopb.Sample{
					{TimestampMs: 5_000, Value: 5 * 2},
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
		expectedL1: []*tempopb.TimeSeries{
			{
				Labels: []common_v1.KeyValue{tempopb.MakeKeyValueString("__name__", "count_over_time")},
				Samples: []tempopb.Sample{
					{TimestampMs: 500, Value: 0},
					{TimestampMs: 1000, Value: 1},
					{TimestampMs: 1500, Value: 0},
					{TimestampMs: 2000, Value: 1},
					{TimestampMs: 2500, Value: 0},
					{TimestampMs: 3000, Value: 1},
				},
			},
		},
		expectedL2: []*tempopb.TimeSeries{
			{
				Labels: []common_v1.KeyValue{tempopb.MakeKeyValueString("__name__", "count_over_time")},
				Samples: []tempopb.Sample{
					{TimestampMs: 500, Value: 0},
					{TimestampMs: 1000, Value: 2 * 1},
					{TimestampMs: 1500, Value: 0},
					{TimestampMs: 2000, Value: 2 * 1},
					{TimestampMs: 2500, Value: 0},
					{TimestampMs: 3000, Value: 2 * 1},
				},
			},
		},
	},
	{
		name: "aligned start is not zero",
		req: &tempopb.QueryRangeRequest{
			Start: 20 * uint64(time.Second),
			End:   50 * uint64(time.Second),
			Step:  10 * uint64(time.Second),
			Query: `{ } | count_over_time()`,
		},
		expectedL1: []*tempopb.TimeSeries{
			{
				Labels: []common_v1.KeyValue{tempopb.MakeKeyValueString("__name__", "count_over_time")},
				Samples: []tempopb.Sample{
					{TimestampMs: 30_000, Value: 10},
					{TimestampMs: 40_000, Value: 10},
					{TimestampMs: 50_000, Value: 10},
				},
			},
		},
		expectedL2: []*tempopb.TimeSeries{
			{
				Labels: []common_v1.KeyValue{tempopb.MakeKeyValueString("__name__", "count_over_time")},
				Samples: []tempopb.Sample{
					{TimestampMs: 30_000, Value: 2 * 10},
					{TimestampMs: 40_000, Value: 2 * 10},
					{TimestampMs: 50_000, Value: 2 * 10},
				},
			},
		},
	},
	{
		name: "not aligned start is not zero",
		req: &tempopb.QueryRangeRequest{
			Start: 21 * uint64(time.Second),
			End:   50 * uint64(time.Second),
			Step:  10 * uint64(time.Second),
			Query: `{ } | count_over_time()`,
		},
		expectedL1: []*tempopb.TimeSeries{
			{
				Labels: []common_v1.KeyValue{tempopb.MakeKeyValueString("__name__", "count_over_time")},
				Samples: []tempopb.Sample{
					{TimestampMs: 30_000, Value: 9},
					{TimestampMs: 40_000, Value: 10},
					{TimestampMs: 50_000, Value: 10},
				},
			},
		},
		expectedL2: []*tempopb.TimeSeries{
			{
				Labels: []common_v1.KeyValue{tempopb.MakeKeyValueString("__name__", "count_over_time")},
				Samples: []tempopb.Sample{
					{TimestampMs: 30_000, Value: 2 * 9},
					{TimestampMs: 40_000, Value: 2 * 10},
					{TimestampMs: 50_000, Value: 2 * 10},
				},
			},
		},
	},
}

var expectedCompareTs = []*tempopb.TimeSeries{
	{
		Labels: []common_v1.KeyValue{
			tempopb.MakeKeyValueString("__meta_type", "baseline"),
			tempopb.MakeKeyValueString("resource.service.name", "odd"),
		},
		Samples: []tempopb.Sample{
			{TimestampMs: 15_000, Value: 8},
			{TimestampMs: 30_000, Value: 7},
			{TimestampMs: 45_000, Value: 8},
			{TimestampMs: 60_000, Value: 2},
		},
	},
	{
		Labels: []common_v1.KeyValue{
			tempopb.MakeKeyValueString("__meta_type", "baseline_total"),
			tempopb.MakeKeyValueString("resource.service.name", "nil"),
		},
		Samples: []tempopb.Sample{
			{TimestampMs: 15_000, Value: 8},
			{TimestampMs: 30_000, Value: 7},
			{TimestampMs: 45_000, Value: 8},
			{TimestampMs: 60_000, Value: 2},
		},
	},
	{
		Labels: []common_v1.KeyValue{
			tempopb.MakeKeyValueString("__meta_type", "selection"),
			tempopb.MakeKeyValueString("resource.service.name", "even"),
		},
		Samples: []tempopb.Sample{
			{TimestampMs: 15_000, Value: 7},
			{TimestampMs: 30_000, Value: 8},
			{TimestampMs: 45_000, Value: 7},
			{TimestampMs: 60_000, Value: 3},
		},
	},
	{
		Labels: []common_v1.KeyValue{
			tempopb.MakeKeyValueString("__meta_type", "selection_total"),
			tempopb.MakeKeyValueString("resource.service.name", "nil"),
		},
		Samples: []tempopb.Sample{
			{TimestampMs: 15_000, Value: 7},
			{TimestampMs: 30_000, Value: 8},
			{TimestampMs: 45_000, Value: 7},
			{TimestampMs: 60_000, Value: 3},
		},
	},
}

// TestTempoDBQueryRange tests the metrics query functionality of TempoDB by verifying various types of
// time-series queries on trace data. The test:
//
// 1. Sets up a test environment
//
// 2. Generates test data:
//   - 100 test spans distributed across 1-100 seconds
//   - Each span's duration equals its start time (e.g. span at 2s has duration 2s)
//   - Spans are tagged with service names "even" or "odd" based on their start time
//
// 3. Tests various query types:
//   - Rate queries (rate(), rate() with filters)
//   - Count queries (count_over_time(), count_over_time() by service)
//   - Statistical queries (min, max, avg, sum, quantile, histogram)
//   - Edge cases with different time ranges and step sizes
//
// 4. Validates results at three processing levels covering the whole query pipeline:
//   - Level 1: Initial query results
//   - Level 2: Results after first aggregation simulating multiple sources
//   - Level 3: Final aggregation results
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
			BloomFP:             .01,
			BloomShardSizeBytes: 100_000,
			Version:             blockVersion,
			RowGroupSizeBytes:   10000,
			DedicatedColumns:    dc,
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
		MaxCompactionRange:      time.Hour,
		BlockRetention:          0,
		CompactedBlockRetention: 0,
	}, &mockSharder{}, &mockOverrides{})
	require.NoError(t, err)

	ctx := context.Background()
	r.EnablePolling(ctx, &mockJobSharder{}, false)

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
			e := traceql.NewEngine()
			eval, err := e.CompileMetricsQueryRange(tc.req, 0, 0, false)
			require.NoError(t, err)

			err = eval.Do(ctx, f, 0, 0, 0)
			require.NoError(t, err)

			actual := eval.Results().ToProto(tc.req)
			expected := tc.expectedL1

			// Slice order is not deterministic, so we sort the slices before comparing
			sortTimeSeries(actual)
			sortTimeSeries(expected)

			if diff := cmp.Diff(expected, actual, floatComparer); diff != "" {
				t.Errorf("Unexpected results for Level 1 processing. Query: %v\n Diff: %v", tc.req.Query, diff)
			}

			evalLevel2, err := e.CompileMetricsQueryRangeNonRaw(tc.req, traceql.AggregateModeSum)
			require.NoError(t, err)
			evalLevel2.ObserveSeries(actual)
			evalLevel2.ObserveSeries(actual) // emulate merging from two sources
			actual = evalLevel2.Results().ToProto(tc.req)
			sortTimeSeries(actual)

			if tc.expectedL2 != nil {
				expected = tc.expectedL2
				sortTimeSeries(expected)
			}

			if diff := cmp.Diff(expected, actual, floatComparer); diff != "" {
				t.Errorf("Unexpected results for Level 2 processing. Query: %v\n Diff: %v", tc.req.Query, diff)
			}

			evalLevel3, err := e.CompileMetricsQueryRangeNonRaw(tc.req, traceql.AggregateModeFinal)
			require.NoError(t, err)
			evalLevel3.ObserveSeries(actual)
			actual = evalLevel3.Results().ToProto(tc.req)
			sortTimeSeries(actual)

			if tc.expectedL3 != nil {
				expected = tc.expectedL3
				sortTimeSeries(expected)
			}

			if diff := cmp.Diff(expected, actual, floatComparer); diff != "" {
				t.Errorf("Unexpected results for Level 3 processing. Query: %v\n Diff: %v", tc.req.Query, diff)
			}
		})
	}

	t.Run("compare", func(t *testing.T) {
		// compare operation generates enormous amount of time series,
		// so we filter by service.name to test at least part of the results
		req := requestWithDefaultRange(`{} | compare({ .service.name="even" })`)
		e := traceql.NewEngine()

		// Level 1
		eval, err := e.CompileMetricsQueryRange(req, 0, 0, false)
		require.NoError(t, err)

		err = eval.Do(ctx, f, 0, 0, 0)
		require.NoError(t, err)

		actual := eval.Results().ToProto(req)

		const labelName = `resource.service.name`
		targetTs := filterTimeSeriesByLabel(actual, labelName)
		sortTimeSeries(targetTs)
		require.Equal(t, expectedCompareTs, targetTs)

		// Level 2
		evalLevel2, err := e.CompileMetricsQueryRangeNonRaw(req, traceql.AggregateModeSum)
		require.NoError(t, err)
		evalLevel2.ObserveSeries(actual)
		actual = evalLevel2.Results().ToProto(req)

		targetTs = filterTimeSeriesByLabel(actual, labelName)
		sortTimeSeries(targetTs)
		require.Equal(t, expectedCompareTs, targetTs)

		// Level 3
		evalLevel3, err := e.CompileMetricsQueryRangeNonRaw(req, traceql.AggregateModeFinal)
		require.NoError(t, err)
		evalLevel3.ObserveSeries(actual)
		actual = evalLevel3.Results().ToProto(req)

		targetTs = filterTimeSeriesByLabel(actual, labelName)
		sortTimeSeries(targetTs)
		require.Equal(t, expectedCompareTs, targetTs)
	})
}

func sortTimeSeries(ts []*tempopb.TimeSeries) {
	sort.Slice(ts, func(i, j int) bool {
		if len(ts[i].Labels) != len(ts[j].Labels) {
			return len(ts[i].Labels) < len(ts[j].Labels)
		}

		for l := range ts[i].Labels {
			if ts[i].Labels[l].Key != ts[j].Labels[l].Key {
				return ts[i].Labels[l].Key < ts[j].Labels[l].Key
			}
			if ts[i].Labels[l].Value != ts[j].Labels[l].Value {
				return ts[i].Labels[l].Value.String() < ts[j].Labels[l].Value.String()
			}
		}

		// All equal
		return false
	})
}

var floatComparer = cmp.Comparer(func(x, y float64) bool {
	return math.Abs(x-y) < 1e-6
})

// filterTimeSeriesByLabel filters the time series to the ones including the given label name.
func filterTimeSeriesByLabel(ts []*tempopb.TimeSeries, labelName string) []*tempopb.TimeSeries {
	var targetTs []*tempopb.TimeSeries
	for _, ts := range ts {
		for _, l := range ts.Labels {
			if l.Key == labelName {
				targetTs = append(targetTs, ts)
				break
			}
		}
	}
	return targetTs
}

// TestTempoDBBatchQueryRange tests the batch metrics query functionality.
// This test uses vparquet5 which fully supports the MatchedGroups feature.
func TestTempoDBBatchQueryRange(t *testing.T) {
	var (
		tempDir      = t.TempDir()
		blockVersion = vparquet5.VersionString
	)

	dc := backend.DedicatedColumns{
		{Scope: "resource", Name: "res-dedicated.01", Type: "string"},
		{Scope: "span", Name: "span-dedicated.01", Type: "string"},
	}
	r, w, _, err := New(&Config{
		Backend: backend.Local,
		Local: &local.Config{
			Path: path.Join(tempDir, "traces"),
		},
		Block: &common.BlockConfig{
			BloomFP:             .01,
			BloomShardSizeBytes: 100_000,
			Version:             blockVersion,
			RowGroupSizeBytes:   10000,
			DedicatedColumns:    dc,
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

	ctx := context.Background()
	r.EnablePolling(ctx, &mockJobSharder{}, false)

	// Write to wal
	wal := w.WAL()

	meta := &backend.BlockMeta{BlockID: backend.NewUUID(), TenantID: testTenantID, DedicatedColumns: dc}
	head, err := wal.NewBlock(meta, model.CurrentEncoding)
	require.NoError(t, err)
	dec := model.MustNewSegmentDecoder(model.CurrentEncoding)

	totalSpans := 50
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

	t.Run("basic_batch_query", func(t *testing.T) {
		// Test metrics math query through L1, L2, L3 pipeline:
		// Fragment 1: only even service spans (25 spans)
		// Fragment 2: all spans (50 spans)
		// Math: even / all
		req := &tempopb.QueryRangeRequest{
			Start: 1,
			End:   50 * uint64(time.Second),
			Step:  50 * uint64(time.Second),
			Query: `({ .service.name="even" } | count_over_time()) / ({ } | count_over_time())`,
		}

		e := traceql.NewEngine()

		// Level 1: raw span processing
		eval, err := e.CompileMetricsQueryRange(req, 0, 0, false)
		require.NoError(t, err)

		err = eval.Do(ctx, f, 0, 0, 0)
		require.NoError(t, err)

		actual := eval.Results().ToProto(req)
		sortTimeSeries(actual)

		// L1: Should have 2 fragment-labeled series
		require.Len(t, actual, 2, "L1: Expected 2 time series (one per fragment)")

		for _, ts := range actual {
			found := false
			for _, l := range ts.Labels {
				if l.Key == "_meta_query_fragment" {
					found = true
					break
				}
			}
			require.True(t, found, "L1: Expected _meta_query_fragment label in series")
		}

		var evenSeries, allSeries *tempopb.TimeSeries
		for _, ts := range actual {
			for _, l := range ts.Labels {
				if l.Key == "_meta_query_fragment" {
					if l.Value.GetStringValue() == "{ .service.name = `even` } | count_over_time()" {
						evenSeries = ts
					} else if l.Value.GetStringValue() == "{ true } | count_over_time()" {
						allSeries = ts
					}
				}
			}
		}

		require.NotNil(t, evenSeries, "L1: Expected series for even service filter")
		require.NotNil(t, allSeries, "L1: Expected series for all spans filter")
		require.Len(t, evenSeries.Samples, 1)
		require.Len(t, allSeries.Samples, 1)
		require.InDelta(t, 25.0, evenSeries.Samples[0].Value, 0.1, "L1: Expected 25 even spans")
		require.InDelta(t, 50.0, allSeries.Samples[0].Value, 0.1, "L1: Expected 50 total spans")

		// Level 2: sum mode (emulate merging from two sources)
		evalLevel2, err := e.CompileMetricsQueryRangeNonRaw(req, traceql.AggregateModeSum)
		require.NoError(t, err)
		evalLevel2.ObserveSeries(actual)
		evalLevel2.ObserveSeries(actual) // observe twice to emulate two sources
		actual = evalLevel2.Results().ToProto(req)
		sortTimeSeries(actual)

		// L2: Should still have 2 fragment-labeled series with doubled values
		require.Len(t, actual, 2, "L2: Expected 2 time series (one per fragment)")

		evenSeries = nil
		allSeries = nil
		for _, ts := range actual {
			for _, l := range ts.Labels {
				if l.Key == "_meta_query_fragment" {
					if l.Value.GetStringValue() == "{ .service.name = `even` } | count_over_time()" {
						evenSeries = ts
					} else if l.Value.GetStringValue() == "{ true } | count_over_time()" {
						allSeries = ts
					}
				}
			}
		}

		require.NotNil(t, evenSeries, "L2: Expected series for even service filter")
		require.NotNil(t, allSeries, "L2: Expected series for all spans filter")
		require.Len(t, evenSeries.Samples, 1)
		require.Len(t, allSeries.Samples, 1)
		require.InDelta(t, 50.0, evenSeries.Samples[0].Value, 0.1, "L2: Expected 50 even spans (25*2)")
		require.InDelta(t, 100.0, allSeries.Samples[0].Value, 0.1, "L2: Expected 100 total spans (50*2)")

		// Level 3: final mode (apply math: 50/100 = 0.5)
		evalLevel3, err := e.CompileMetricsQueryRangeNonRaw(req, traceql.AggregateModeFinal)
		require.NoError(t, err)
		evalLevel3.ObserveSeries(actual)
		actual = evalLevel3.Results().ToProto(req)

		// L3: Should have 1 series with the division result and no fragment labels
		require.Len(t, actual, 1, "L3: Expected 1 time series (math result)")
		for _, l := range actual[0].Labels {
			require.NotEqual(t, "_meta_query_fragment", l.Key, "L3: Should not have _meta_query_fragment label")
		}
		require.Len(t, actual[0].Samples, 1)
		require.InDelta(t, 0.5, actual[0].Samples[0].Value, 0.01, "L3: Expected 50/100 = 0.5")
	})

	t.Run("overlapping_matches", func(t *testing.T) {
		// Test L1/L2/L3 pipeline with 3-leaf math expression: (even + odd) + all
		req := &tempopb.QueryRangeRequest{
			Start: 1,
			End:   50 * uint64(time.Second),
			Step:  50 * uint64(time.Second),
			Query: `(({ .service.name="even" } | count_over_time()) + ({ .service.name="odd" } | count_over_time())) + ({ } | count_over_time())`,
		}

		e := traceql.NewEngine()

		// Level 1
		eval, err := e.CompileMetricsQueryRange(req, 0, 0, false)
		require.NoError(t, err)

		err = eval.Do(ctx, f, 0, 0, 0)
		require.NoError(t, err)

		actual := eval.Results().ToProto(req)
		require.Len(t, actual, 3, "L1: Expected 3 time series")

		var evenCount, oddCount, totalCount float64
		for _, ts := range actual {
			for _, l := range ts.Labels {
				if l.Key == "_meta_query_fragment" {
					switch l.Value.GetStringValue() {
					case "{ .service.name = `even` } | count_over_time()":
						evenCount = ts.Samples[0].Value
					case "{ .service.name = `odd` } | count_over_time()":
						oddCount = ts.Samples[0].Value
					case "{ true } | count_over_time()":
						totalCount = ts.Samples[0].Value
					}
				}
			}
		}

		require.InDelta(t, 25.0, evenCount, 0.1, "L1: even")
		require.InDelta(t, 25.0, oddCount, 0.1, "L1: odd")
		require.InDelta(t, 50.0, totalCount, 0.1, "L1: all")
		require.InDelta(t, totalCount, evenCount+oddCount, 0.1, "L1: even + odd should equal total")

		// Level 2: sum mode (observe twice)
		evalLevel2, err := e.CompileMetricsQueryRangeNonRaw(req, traceql.AggregateModeSum)
		require.NoError(t, err)
		evalLevel2.ObserveSeries(actual)
		evalLevel2.ObserveSeries(actual)
		actual = evalLevel2.Results().ToProto(req)

		require.Len(t, actual, 3, "L2: Expected 3 time series")

		evenCount, oddCount, totalCount = 0, 0, 0
		for _, ts := range actual {
			for _, l := range ts.Labels {
				if l.Key == "_meta_query_fragment" {
					switch l.Value.GetStringValue() {
					case "{ .service.name = `even` } | count_over_time()":
						evenCount = ts.Samples[0].Value
					case "{ .service.name = `odd` } | count_over_time()":
						oddCount = ts.Samples[0].Value
					case "{ true } | count_over_time()":
						totalCount = ts.Samples[0].Value
					}
				}
			}
		}

		require.InDelta(t, 50.0, evenCount, 0.1, "L2: even (25*2)")
		require.InDelta(t, 50.0, oddCount, 0.1, "L2: odd (25*2)")
		require.InDelta(t, 100.0, totalCount, 0.1, "L2: all (50*2)")

		// Level 3: final mode (apply math: (50+50)+100 = 200)
		evalLevel3, err := e.CompileMetricsQueryRangeNonRaw(req, traceql.AggregateModeFinal)
		require.NoError(t, err)
		evalLevel3.ObserveSeries(actual)
		actual = evalLevel3.Results().ToProto(req)

		require.Len(t, actual, 1, "L3: Expected 1 time series (math result)")
		for _, l := range actual[0].Labels {
			require.NotEqual(t, "_meta_query_fragment", l.Key, "L3: Should not have _meta_query_fragment label")
		}
		require.Len(t, actual[0].Samples, 1)
		require.InDelta(t, 200.0, actual[0].Samples[0].Value, 0.1, "L3: Expected (50+50)+100 = 200")
	})

	t.Run("no_match_all_fragment", func(t *testing.T) {
		// Regression test: queries without a {} fragment previously returned zeroes
		// because IntrinsicSpanStartTime was added to SecondPassConditions instead
		// of Conditions. With no SecondPass callback in the batch path, the storage
		// skipped the second pass entirely, leaving StartTimeUnixNanos() = 0 for
		// all spans, causing them all to be filtered out by the time check.
		req := &tempopb.QueryRangeRequest{
			Start: 1,
			End:   50 * uint64(time.Second),
			Step:  50 * uint64(time.Second),
			Query: `({ .service.name="even" } | count_over_time()) + ({ .service.name="odd" } | count_over_time())`,
		}

		e := traceql.NewEngine()

		// Level 1
		eval, err := e.CompileMetricsQueryRange(req, 0, 0, false)
		require.NoError(t, err)

		err = eval.Do(ctx, f, 0, 0, 0)
		require.NoError(t, err)

		actual := eval.Results().ToProto(req)
		require.Len(t, actual, 2, "L1: Expected 2 time series")

		var evenCount, oddCount float64
		for _, ts := range actual {
			for _, l := range ts.Labels {
				if l.Key == "_meta_query_fragment" {
					switch l.Value.GetStringValue() {
					case "{ .service.name = `even` } | count_over_time()":
						evenCount = ts.Samples[0].Value
					case "{ .service.name = `odd` } | count_over_time()":
						oddCount = ts.Samples[0].Value
					}
				}
			}
		}

		require.InDelta(t, 25.0, evenCount, 0.1, "L1: even count must be 25")
		require.InDelta(t, 25.0, oddCount, 0.1, "L1: odd count must be 25")

		// Level 2: sum mode (observe twice)
		evalLevel2, err := e.CompileMetricsQueryRangeNonRaw(req, traceql.AggregateModeSum)
		require.NoError(t, err)
		evalLevel2.ObserveSeries(actual)
		evalLevel2.ObserveSeries(actual)
		actual = evalLevel2.Results().ToProto(req)

		require.Len(t, actual, 2, "L2: Expected 2 time series")

		evenCount, oddCount = 0, 0
		for _, ts := range actual {
			for _, l := range ts.Labels {
				if l.Key == "_meta_query_fragment" {
					switch l.Value.GetStringValue() {
					case "{ .service.name = `even` } | count_over_time()":
						evenCount = ts.Samples[0].Value
					case "{ .service.name = `odd` } | count_over_time()":
						oddCount = ts.Samples[0].Value
					}
				}
			}
		}

		require.InDelta(t, 50.0, evenCount, 0.1, "L2: even (25*2)")
		require.InDelta(t, 50.0, oddCount, 0.1, "L2: odd (25*2)")

		// Level 3: final mode (apply math: 50+50 = 100)
		evalLevel3, err := e.CompileMetricsQueryRangeNonRaw(req, traceql.AggregateModeFinal)
		require.NoError(t, err)
		evalLevel3.ObserveSeries(actual)
		actual = evalLevel3.Results().ToProto(req)

		require.Len(t, actual, 1, "L3: Expected 1 time series (math result)")
		for _, l := range actual[0].Labels {
			require.NotEqual(t, "_meta_query_fragment", l.Key, "L3: Should not have _meta_query_fragment label")
		}
		require.Len(t, actual[0].Samples, 1)
		require.InDelta(t, 100.0, actual[0].Samples[0].Value, 0.1, "L3: Expected 50+50 = 100")
	})

	t.Run("validation_errors", func(t *testing.T) {
		e := traceql.NewEngine()

		// Test: non-metrics query
		req := &tempopb.QueryRangeRequest{
			Start: 1,
			End:   50 * uint64(time.Second),
			Step:  50 * uint64(time.Second),
			Query: `{ .service.name="test" }`,
		}
		_, err := e.CompileMetricsQueryRange(req, 0, 0, false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "not a metrics query")

		// Test: invalid query syntax
		req.Query = `{ invalid }`
		_, err = e.CompileMetricsQueryRange(req, 0, 0, false)
		require.Error(t, err)
	})
}

// TestTempoDBBatchQueryRangeFallback tests that batch queries work correctly
// with vparquet4 which returns 0 for MatchedGroups (fallback evaluation).
func TestTempoDBBatchQueryRangeFallback(t *testing.T) {
	var (
		tempDir      = t.TempDir()
		blockVersion = vparquet4.VersionString // Use vparquet4 which has stub MatchedGroups
	)

	r, w, _, err := New(&Config{
		Backend: backend.Local,
		Local: &local.Config{
			Path: path.Join(tempDir, "traces"),
		},
		Block: &common.BlockConfig{
			BloomFP:             .01,
			BloomShardSizeBytes: 100_000,
			Version:             blockVersion,
			RowGroupSizeBytes:   10000,
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

	ctx := context.Background()
	r.EnablePolling(ctx, &mockJobSharder{}, false)

	wal := w.WAL()

	meta := &backend.BlockMeta{BlockID: backend.NewUUID(), TenantID: testTenantID}
	head, err := wal.NewBlock(meta, model.CurrentEncoding)
	require.NoError(t, err)
	dec := model.MustNewSegmentDecoder(model.CurrentEncoding)

	totalSpans := 20
	for i := 1; i <= totalSpans; i++ {
		tid := test.ValidTraceID(nil)
		sp := test.MakeSpan(tid)
		sp.StartTimeUnixNano = uint64(i * int(time.Second))
		sp.EndTimeUnixNano = sp.StartTimeUnixNano + uint64(i*int(time.Second))

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
							Spans: []*v1.Span{sp},
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

	block, err := w.CompleteBlock(context.Background(), head)
	require.NoError(t, err)

	f := traceql.NewSpansetFetcherWrapper(func(ctx context.Context, req traceql.FetchSpansRequest) (traceql.FetchSpansResponse, error) {
		return block.Fetch(ctx, req, common.DefaultSearchOptions())
	})

	// Test that fallback works - vparquet4 returns 0 for MatchedGroups,
	// so the evaluator should fall back to direct filter evaluation.
	// Uses CompileMetricsQueryRange with a math expression query.
	req := &tempopb.QueryRangeRequest{
		Start: 1,
		End:   20 * uint64(time.Second),
		Step:  20 * uint64(time.Second),
		Query: `({ .service.name="even" } | count_over_time()) / ({ } | count_over_time())`,
	}

	e := traceql.NewEngine()
	eval, err := e.CompileMetricsQueryRange(req, 0, 0, false)
	require.NoError(t, err)

	err = eval.Do(ctx, f, 0, 0, 0)
	require.NoError(t, err)

	actual := eval.Results().ToProto(req)
	require.Len(t, actual, 2, "Expected 2 time series with fallback evaluation")

	// Verify counts are correct even with fallback
	var evenCount, totalCount float64
	for _, ts := range actual {
		for _, l := range ts.Labels {
			if l.Key == "_meta_query_fragment" {
				switch l.Value.GetStringValue() {
				case "{ .service.name = `even` } | count_over_time()":
					evenCount = ts.Samples[0].Value
				case "{ true } | count_over_time()":
					totalCount = ts.Samples[0].Value
				}
			}
		}
	}

	require.InDelta(t, 10.0, evenCount, 0.1, "Expected 10 even spans")
	require.InDelta(t, 20.0, totalCount, 0.1, "Expected 20 total spans")
}

// blockForMetricsBenchmarks opens a local block for benchmarking.
// Requires environment variables:
//
//	BENCH_BLOCKID  - block UUID (e.g. 030c8c4f-9d47-4916-aadc-26b90b1d2bc4)
//	BENCH_PATH     - root backend path (<path>/<tenant>/<block>)
//	BENCH_TENANTID - tenant ID (default "1")
func blockForMetricsBenchmarks(b *testing.B) (common.BackendBlock, *backend.BlockMeta) {
	id, ok := os.LookupEnv("BENCH_BLOCKID")
	if !ok {
		b.Skip("BENCH_BLOCKID is not set. Set it to the block UUID to benchmark against.")
	}

	p, ok := os.LookupEnv("BENCH_PATH")
	if !ok {
		b.Skip("BENCH_PATH is not set. Set it to the root backend path.")
	}

	tenantID, ok := os.LookupEnv("BENCH_TENANTID")
	if !ok {
		tenantID = "1"
	}

	blockID := uuid.MustParse(id)
	r, _, _, err := local.New(&local.Config{
		Path: p,
	})
	require.NoError(b, err)

	rr := backend.NewReader(r)
	meta, err := rr.BlockMeta(context.Background(), blockID, tenantID)
	require.NoError(b, err)

	block, err := encoding.OpenBlock(meta, rr)
	require.NoError(b, err)

	return block, meta
}

// BenchmarkMetricsQueryRange benchmarks metrics query execution against a real block.
//
// Sub-benchmarks:
//
//	math  - MetricsMath expression: two filtered rate() queries combined with +
//	regex - Single query using regex filter to match both values
//	concurrent_5 - Five independent queries executed concurrently
func BenchmarkMetricsQueryRange(b *testing.B) {
	block, meta := blockForMetricsBenchmarks(b)

	var (
		e    = traceql.NewEngine()
		ctx  = context.Background()
		opts = common.DefaultSearchOptions()
		st   = uint64(meta.StartTime.UnixNano())
		end  = uint64(meta.EndTime.UnixNano())
	)

	f := traceql.NewSpansetFetcherWrapper(func(ctx context.Context, req traceql.FetchSpansRequest) (traceql.FetchSpansResponse, error) {
		return block.Fetch(ctx, req, opts)
	})

	newReq := func(query string) *tempopb.QueryRangeRequest {
		return &tempopb.QueryRangeRequest{
			Query: query,
			Step:  uint64(time.Minute),
			Start: st,
			End:   end,
		}
	}

	// runFullPipeline executes the full L1 → L2 → L3 pipeline for a single query.
	runFullPipeline := func(b *testing.B, req *tempopb.QueryRangeRequest) {
		b.Helper()

		b.ResetTimer()
		for b.Loop() {
			// L1: raw span processing
			evalL1, err := e.CompileMetricsQueryRange(req, 0, 0, false)
			require.NoError(b, err)

			err = evalL1.Do(ctx, f, st, end, 0)
			require.NoError(b, err)

			l1Series := evalL1.Results().ToProto(req)

			// L2: sum mode (simulates combining from multiple queriers)
			evalL2, err := e.CompileMetricsQueryRangeNonRaw(req, traceql.AggregateModeSum)
			require.NoError(b, err)

			evalL2.ObserveSeries(l1Series)
			l2Series := evalL2.Results().ToProto(req)

			// L3: final mode (applies math for MetricsMath, finalizes aggregates)
			evalL3, err := e.CompileMetricsQueryRangeNonRaw(req, traceql.AggregateModeFinal)
			require.NoError(b, err)

			evalL3.ObserveSeries(l2Series)
			_ = evalL3.Results()
		}
	}

	// b.Run("temp", func(b *testing.B) {
	// 	query := `{ } | count_over_time() by (resource.service.name)`
	// 	// query := `{ } | count_over_time()`
	// 	req := newReq(query)
	// 	runFullPipeline(b, req)
	// })

	b.Run("math", func(b *testing.B) {
		query := `({resource.service.name="gamma-operator"} | count_over_time()) + ({resource.service.name="modelapi"} | count_over_time()) + ({resource.service.name="loki-ruler"} | count_over_time()) + ({resource.service.name="service-model-operator"} | count_over_time()) + ({resource.service.name="tempo-block-builder"} | count_over_time())`
		req := newReq(query)
		runFullPipeline(b, req)
	})

	b.Run("regex", func(b *testing.B) {
		query := `{resource.service.name=~"gamma-operator|modelapi|loki-ruler|service-model-operator|tempo-block-builder"} | count_over_time()`
		req := newReq(query)
		runFullPipeline(b, req)
	})

	b.Run("regex all cond false", func(b *testing.B) {
		query := `{resource.service.name=~"gamma-operator|modelapi|loki-ruler|service-model-operator|tempo-block-builder" || resource.host.name = "it will not find Im sure"} | count_over_time()`
		req := newReq(query)
		runFullPipeline(b, req)
	})

	b.Run("ORs", func(b *testing.B) {
		query := `{resource.service.name="gamma-operator" || resource.host.name != "it will not find Im sure" || resource.ip!="loki-ruler" || resource.k8s.cluster.name!="service-model-operator" || resource.k8s.container.name!="tempo-block-builder"} | count_over_time()`
		req := newReq(query)
		runFullPipeline(b, req)
	})

	b.Run("math different conditions", func(b *testing.B) {
		query := `({resource.service.name="gamma-operator"} | count_over_time()) + ({resource.host.name != "it will not find Im sure"} | count_over_time()) + ({resource.ip!="loki-ruler"} | count_over_time()) + ({resource.k8s.cluster.name!="service-model-operator"} | count_over_time()) + ({resource.k8s.container.name!="tempo-block-builder"} | count_over_time())`
		req := newReq(query)
		runFullPipeline(b, req)
	})

	b.Run("concurrent_5", func(b *testing.B) {
		queries := []string{
			`{resource.service.name="gamma-operator"} | count_over_time()`,
			`{resource.service.name="modelapi"} | count_over_time()`,
			`{resource.service.name="loki-ruler"} | count_over_time()`,
			`{resource.service.name="service-model-operator"} | count_over_time()`,
			`{resource.service.name="tempo-block-builder"} | count_over_time()`,
		}

		reqs := make([]*tempopb.QueryRangeRequest, len(queries))
		for i, q := range queries {
			reqs[i] = newReq(q)
		}

		b.ResetTimer()
		for b.Loop() {
			var wg sync.WaitGroup
			errs := make([]error, len(reqs))

			for i, req := range reqs {
				wg.Add(1)
				go func(idx int, r *tempopb.QueryRangeRequest) {
					defer wg.Done()

					evalL1, err := e.CompileMetricsQueryRange(r, 0, 0, false)
					if err != nil {
						errs[idx] = err
						return
					}

					if err = evalL1.Do(ctx, f, st, end, 0); err != nil {
						errs[idx] = err
						return
					}

					l1Series := evalL1.Results().ToProto(r)

					evalL2, err := e.CompileMetricsQueryRangeNonRaw(r, traceql.AggregateModeSum)
					if err != nil {
						errs[idx] = err
						return
					}
					evalL2.ObserveSeries(l1Series)
					l2Series := evalL2.Results().ToProto(r)

					evalL3, err := e.CompileMetricsQueryRangeNonRaw(r, traceql.AggregateModeFinal)
					if err != nil {
						errs[idx] = err
						return
					}
					evalL3.ObserveSeries(l2Series)
					_ = evalL3.Results()
				}(i, req)
			}

			wg.Wait()

			for i, err := range errs {
				require.NoError(b, err, fmt.Sprintf("query %d failed: %s", i, queries[i]))
			}
		}
	})
}

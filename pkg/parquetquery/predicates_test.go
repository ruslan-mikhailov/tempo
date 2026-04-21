package parquetquery

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"
)

type testInt struct {
	N int64 `parquet:",dict"`
}

type mockPredicate struct {
	ret         bool
	valCalled   bool
	pageCalled  bool
	chunkCalled bool
}

type testDictString struct {
	S string `parquet:",dict"`
}

var _ Predicate = (*mockPredicate)(nil)

func newAlwaysTruePredicate() *mockPredicate {
	return &mockPredicate{ret: true}
}

func newAlwaysFalsePredicate() *mockPredicate {
	return &mockPredicate{ret: false}
}

func (p *mockPredicate) String() string                          { return "mockPredicate{}" }
func (p *mockPredicate) KeepValue(parquet.Value) bool            { p.valCalled = true; return p.ret }
func (p *mockPredicate) KeepPage(parquet.Page) bool              { p.pageCalled = true; return p.ret }
func (p *mockPredicate) KeepColumnChunk(*ColumnChunkHelper) bool { p.chunkCalled = true; return p.ret }

type predicateTestCase struct {
	testName   string
	writeData  func(w *parquet.Writer) //nolint:all
	keptChunks int
	keptPages  int
	keptValues int
	predicate  Predicate
}

func TestSubstringPredicate(t *testing.T) {
	testCases := []predicateTestCase{
		{
			testName:   "all chunks/pages/values inspected",
			predicate:  NewSubstringPredicate("b"),
			keptChunks: 1,
			keptPages:  1,
			keptValues: 2,
			writeData: func(w *parquet.Writer) { //nolint:all

				require.NoError(t, w.Write(&testDictString{"abc"})) // kept
				require.NoError(t, w.Write(&testDictString{"bcd"})) // kept
				require.NoError(t, w.Write(&testDictString{"cde"})) // skipped
			},
		},
		{
			testName:   "dictionary in the page header allows for skipping a page",
			predicate:  NewSubstringPredicate("x"), // Not present in any values
			keptChunks: 0,
			keptPages:  0,
			keptValues: 0,
			writeData: func(w *parquet.Writer) { //nolint:all
				require.NoError(t, w.Write(&testDictString{"abc"}))
				require.NoError(t, w.Write(&testDictString{"abc"}))
				require.NoError(t, w.Write(&testDictString{"abc"}))
				require.NoError(t, w.Write(&testDictString{"abc"}))
				require.NoError(t, w.Write(&testDictString{"abc"}))
			},
		},
	}

	for _, tC := range testCases {
		t.Run(tC.testName, func(t *testing.T) {
			testPredicate(t, tC)
		})
	}
}

func TestNewStringInPredicate(t *testing.T) {
	testCases := []predicateTestCase{
		{
			testName: "all chunks/pages/values inspected",
			predicate: func() Predicate {
				return NewStringInPredicate([]string{"abc", "acd"})
			}(),
			keptChunks: 1,
			keptPages:  1,
			keptValues: 2,
			writeData: func(w *parquet.Writer) { //nolint:all
				require.NoError(t, w.Write(&testDictString{"abc"})) // kept
				require.NoError(t, w.Write(&testDictString{"acd"})) // kept
				require.NoError(t, w.Write(&testDictString{"cde"})) // skipped
			},
		},
		{
			testName: "dictionary in the page header allows for skipping a column chunk",
			predicate: func() Predicate {
				return NewStringInPredicate([]string{"x"})
			}(), // Not present in any values
			keptChunks: 0,
			keptPages:  0,
			keptValues: 0,
			writeData: func(w *parquet.Writer) { //nolint:all
				require.NoError(t, w.Write(&testDictString{"abc"}))
				require.NoError(t, w.Write(&testDictString{"abc"}))
			},
		},
	}

	for _, tC := range testCases {
		t.Run(tC.testName, func(t *testing.T) {
			testPredicate(t, tC)
		})
	}
}

func TestNewRegexInPredicate(t *testing.T) {
	testCases := []predicateTestCase{
		{
			testName: "all chunks/pages/values inspected",
			predicate: func() Predicate {
				pred, err := NewRegexInPredicate([]string{"a.*"})
				require.NoError(t, err)

				return pred
			}(),
			keptChunks: 1,
			keptPages:  1,
			keptValues: 2,
			writeData: func(w *parquet.Writer) { //nolint:all
				require.NoError(t, w.Write(&testDictString{"abc"})) // kept
				require.NoError(t, w.Write(&testDictString{"acd"})) // kept
				require.NoError(t, w.Write(&testDictString{"cde"})) // skipped
			},
		},
		{
			testName: "dictionary in the page header allows for skipping a column chunk",
			predicate: func() Predicate {
				pred, err := NewRegexInPredicate([]string{"x.*"})
				require.NoError(t, err)

				return pred
			}(), // Not present in any values
			keptChunks: 0,
			keptPages:  0,
			keptValues: 0,
			writeData: func(w *parquet.Writer) { //nolint:all
				require.NoError(t, w.Write(&testDictString{"abc"}))
				require.NoError(t, w.Write(&testDictString{"abc"}))
			},
		},
	}

	for _, tC := range testCases {
		t.Run(tC.testName, func(t *testing.T) {
			testPredicate(t, tC)
		})
	}
}

func TestNewStringNotInPredicate(t *testing.T) {
	testCases := []predicateTestCase{
		{
			testName: "all chunks/pages/values inspected",
			predicate: func() Predicate {
				return NewStringNotInPredicate([]string{"abc", "acd"})
			}(),
			keptChunks: 1,
			keptPages:  1,
			keptValues: 2,
			writeData: func(w *parquet.Writer) { //nolint:all
				require.NoError(t, w.Write(&testDictString{"abc"})) // skipped
				require.NoError(t, w.Write(&testDictString{"acd"})) // skipped
				require.NoError(t, w.Write(&testDictString{"cde"})) // kept
				require.NoError(t, w.Write(&testDictString{"xde"})) // kept
			},
		},
		{
			testName: "dictionary in the page header allows for skipping a column chunk",
			predicate: func() Predicate {
				return NewStringNotInPredicate([]string{"xyz"})
			}(), // All values excluded
			keptChunks: 0,
			keptPages:  0,
			keptValues: 0,
			writeData: func(w *parquet.Writer) { //nolint:all
				require.NoError(t, w.Write(&testDictString{"xyz"}))
				require.NoError(t, w.Write(&testDictString{"xyz"}))
			},
		},
	}

	for _, tC := range testCases {
		t.Run(tC.testName, func(t *testing.T) {
			testPredicate(t, tC)
		})
	}
}

func TestNewRegexNotInPredicate(t *testing.T) {
	testCases := []predicateTestCase{
		{
			testName: "all chunks/pages/values inspected",
			predicate: func() Predicate {
				pred, err := NewRegexNotInPredicate([]string{"a.*"})
				require.NoError(t, err)

				return pred
			}(),
			keptChunks: 1,
			keptPages:  1,
			keptValues: 2,
			writeData: func(w *parquet.Writer) { //nolint:all
				require.NoError(t, w.Write(&testDictString{"abc"})) // skipped
				require.NoError(t, w.Write(&testDictString{"acd"})) // skipped
				require.NoError(t, w.Write(&testDictString{"cde"})) // kept
				require.NoError(t, w.Write(&testDictString{"xde"})) // kept
			},
		},
		{
			testName: "dictionary in the page header allows for skipping a column chunk",
			predicate: func() Predicate {
				pred, err := NewRegexNotInPredicate([]string{"x.*"})
				require.NoError(t, err)

				return pred
			}(), // Not present in any values
			keptChunks: 0,
			keptPages:  0,
			keptValues: 0,
			writeData: func(w *parquet.Writer) { //nolint:all
				require.NoError(t, w.Write(&testDictString{"xyz"}))
				require.NoError(t, w.Write(&testDictString{"xyz"}))
			},
		},
	}

	for _, tC := range testCases {
		t.Run(tC.testName, func(t *testing.T) {
			testPredicate(t, tC)
		})
	}
}

func TestNewOrPredicateCollapses(t *testing.T) {
	t.Run("nil child collapses to always-true", func(t *testing.T) {
		p := NewOrPredicate(NewStringEqualPredicate("a"), nil, NewStringEqualPredicate("b"))
		_, isAlwaysTrue := p.(alwaysTruePredicate)
		require.True(t, isAlwaysTrue, "a nil child should short-circuit to alwaysTruePredicate")
		require.True(t, p.KeepValue(parquet.ValueOf("anything")))
		require.True(t, p.KeepValue(parquet.NullValue()))
	})

	t.Run("single child returned directly", func(t *testing.T) {
		inner := NewStringEqualPredicate("a")
		p := NewOrPredicate(inner)
		require.Equal(t, inner, p, "single-child Or should return its child unwrapped")
	})

	t.Run("nested Or is flattened", func(t *testing.T) {
		// Use mixed-type children so neither inner nor outer collapse to
		// ByteInPredicate; we just want to see the flattened preds slice.
		inner := NewOrPredicate(
			NewStringEqualPredicate("a"),
			NewIntEqualPredicate(1),
		)
		outer := NewOrPredicate(inner, NewIntEqualPredicate(2))
		op, ok := outer.(*OrPredicate)
		require.True(t, ok, "outer should stay an OrPredicate, got %T", outer)
		require.Len(t, op.preds, 3, "flattening should inline inner's preds")
	})

	t.Run("homogeneous nested Or collapses end-to-end to ByteIn", func(t *testing.T) {
		// The inner Or already collapses to a ByteInPredicate; the outer
		// then sees [ByteInPredicate, ByteEqualPredicate] which is mixed
		// and stays as an OrPredicate. This documents that *pre-flattened*
		// Or of only ByteEquals produces the expected single ByteIn fast path.
		p := NewOrPredicate(
			NewStringEqualPredicate("a"),
			NewStringEqualPredicate("b"),
			NewStringEqualPredicate("c"),
			NewStringEqualPredicate("d"),
		)
		bip, ok := p.(*ByteInPredicate)
		require.True(t, ok, "flat homogeneous Or(ByteEqual…) should collapse to ByteInPredicate, got %T", p)
		require.Len(t, bip.values, 4)
	})

	t.Run("homogeneous ByteEqual children collapse to ByteIn", func(t *testing.T) {
		p := NewOrPredicate(
			NewStringEqualPredicate("a"),
			NewStringEqualPredicate("b"),
		)
		_, ok := p.(*ByteInPredicate)
		require.True(t, ok, "Or(ByteEqual,ByteEqual) should collapse to ByteInPredicate, got %T", p)
		require.True(t, p.KeepValue(parquet.ValueOf("a")))
		require.True(t, p.KeepValue(parquet.ValueOf("b")))
		require.False(t, p.KeepValue(parquet.ValueOf("c")))
	})

	t.Run("mixed children keep Or wrapper", func(t *testing.T) {
		p := NewOrPredicate(
			NewStringEqualPredicate("a"),
			NewIntEqualPredicate(1),
		)
		_, ok := p.(*OrPredicate)
		require.True(t, ok, "heterogeneous Or must remain an OrPredicate, got %T", p)
	})
}

// TestOrPredicateCallsKeepColumnChunk ensures that the OrPredicate calls
// KeepColumnChunk on all of its children. This is important because the
// Dictionary predicates rely on KeepColumnChunk always being called at the
// beginning of a row group to reset their page.
func TestOrPredicateCallsKeepColumnChunk(t *testing.T) {
	tcs := []struct {
		preds []*mockPredicate
	}{
		{},
		{
			preds: []*mockPredicate{
				newAlwaysTruePredicate(),
			},
		},
		{
			preds: []*mockPredicate{
				newAlwaysFalsePredicate(),
			},
		},
		{
			preds: []*mockPredicate{
				newAlwaysFalsePredicate(),
				newAlwaysTruePredicate(),
			},
		},
		{
			preds: []*mockPredicate{
				newAlwaysTruePredicate(),
				newAlwaysFalsePredicate(),
			},
		},
	}

	for _, tc := range tcs {
		preds := make([]Predicate, 0, len(tc.preds)+1)
		for _, pred := range tc.preds {
			preds = append(preds, pred)
		}

		recordPred := &mockPredicate{}
		preds = append(preds, recordPred)

		p := NewOrPredicate(preds...)
		p.KeepColumnChunk(nil)

		for _, pred := range preds {
			require.True(t, pred.(*mockPredicate).chunkCalled)
		}
	}
}

// testPredicate by writing data and then iterating the column.
// The data model must contain a single column.
func testPredicate(t *testing.T, tc predicateTestCase) {
	t.Helper()
	buf := new(bytes.Buffer)
	w := parquet.NewWriter(buf)
	tc.writeData(w)
	w.Flush()
	w.Close()

	file := bytes.NewReader(buf.Bytes())
	r, err := parquet.OpenFile(file, int64(buf.Len()))
	require.NoError(t, err)

	p := InstrumentedPredicate{Pred: tc.predicate}

	i := NewSyncIterator(context.TODO(), r.RowGroups(), 0, SyncIteratorOptPredicate(&p))
	for {
		res, err := i.Next()
		require.NoError(t, err)
		if res == nil {
			break
		}
	}

	require.Equal(t, tc.keptChunks, int(p.KeptColumnChunks), "keptChunks")
	require.Equal(t, tc.keptPages, int(p.KeptPages), "keptPages")
	require.Equal(t, tc.keptValues, int(p.KeptValues), "keptValues")
}

func BenchmarkSubstringPredicate(b *testing.B) {
	p := NewSubstringPredicate("abc")

	s := make([]parquet.Value, 1000)
	for i := 0; i < 1000; i++ {
		s[i] = parquet.ValueOf(uuid.New().String())
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for _, ss := range s {
			p.KeepValue(ss)
		}
	}
}

func BenchmarkStringInPredicate(b *testing.B) {
	// Size matrix reflects the real live-store workload where IN-lists
	// routinely carry many trace/span/attribute values. A single-element
	// predicate is the common "x = 'y'" lowering.
	sizes := []int{1, 4, 16, 64, 256}

	// Mix of matching (10%) and non-matching (90%) inputs so the hot path
	// exercises both early-termination and full scans.
	inputs := make([]parquet.Value, 1000)
	for i := range inputs {
		inputs[i] = parquet.ValueOf(uuid.New().String())
	}

	for _, n := range sizes {
		vals := make([]string, n)
		for i := range vals {
			vals[i] = uuid.New().String()
		}
		// 10% of inputs are set to a value from the predicate to exercise
		// early-return behavior.
		matchingInputs := make([]parquet.Value, len(inputs))
		copy(matchingInputs, inputs)
		for i := 0; i < len(matchingInputs); i += 10 {
			matchingInputs[i] = parquet.ValueOf(vals[i%n])
		}

		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			p := NewStringInPredicate(vals)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for _, ss := range matchingInputs {
					p.KeepValue(ss)
				}
			}
		})
	}
}

func BenchmarkOrPredicateByteEqual(b *testing.B) {
	// OrPredicate of N ByteEquals is the common lowering of
	// `field = "a" || field = "b" || ...`. Measure the raw dispatch cost
	// to confirm that flattening + collapse into ByteInPredicate wins.
	sizes := []int{2, 4, 8, 16}

	inputs := make([]parquet.Value, 1000)
	for i := range inputs {
		inputs[i] = parquet.ValueOf(uuid.New().String())
	}

	for _, n := range sizes {
		preds := make([]Predicate, n)
		for i := range preds {
			preds[i] = NewByteEqualPredicate([]byte(uuid.New().String()))
		}
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			p := NewOrPredicate(preds...)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for _, ss := range inputs {
					p.KeepValue(ss)
				}
			}
		})
	}
}

func BenchmarkRegexInPredicate(b *testing.B) {
	p, err := NewRegexInPredicate([]string{"abc"})
	require.NoError(b, err)

	s := make([]parquet.Value, 1000)
	for i := 0; i < 1000; i++ {
		s[i] = parquet.ValueOf(uuid.New().String())
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for _, ss := range s {
			p.KeepValue(ss)
		}
	}
}

func TestIntInPredicate(t *testing.T) {
	testCases := []predicateTestCase{
		{
			testName: "all chunks/pages/values inspected",
			predicate: func() Predicate {
				var p Predicate = NewIntInPredicate([]int64{1, 3})
				return p
			}(),
			keptChunks: 1,
			keptPages:  1,
			keptValues: 2,
			writeData: func(w *parquet.Writer) { //nolint:all
				require.NoError(t, w.Write(&testInt{1}))  // kept
				require.NoError(t, w.Write(&testInt{2}))  // skipped
				require.NoError(t, w.Write(&testInt{3}))  // kept
				require.NoError(t, w.Write(&testInt{10})) // skipped
			},
		},
		{
			testName: "dictionary allows skipping a column chunk when no ints match",
			predicate: func() Predicate {
				var p Predicate = NewIntInPredicate([]int64{0, 4, 100})
				return p
			}(),
			keptChunks: 0,
			keptPages:  0,
			keptValues: 0,
			writeData: func(w *parquet.Writer) { //nolint:all
				require.NoError(t, w.Write(&testInt{1}))
				require.NoError(t, w.Write(&testInt{2}))
				require.NoError(t, w.Write(&testInt{3}))
			},
		},
	}

	for _, tC := range testCases {
		t.Run(tC.testName, func(t *testing.T) {
			testPredicate(t, tC)
		})
	}
}

func TestIntNotInPredicate(t *testing.T) {
	testCases := []predicateTestCase{
		{
			testName: "all chunks/pages/values inspected",
			predicate: func() Predicate {
				var p Predicate = NewIntNotInPredicate([]int64{1, 3})
				return p
			}(),
			keptChunks: 1,
			keptPages:  1,
			keptValues: 2,
			writeData: func(w *parquet.Writer) { //nolint:all
				require.NoError(t, w.Write(&testInt{1})) // skipped
				require.NoError(t, w.Write(&testInt{2})) // kept
				require.NoError(t, w.Write(&testInt{3})) // skipped
				require.NoError(t, w.Write(&testInt{4})) // kept
			},
		},
		{
			testName: "dictionary allows skipping a column chunk when all ints excluded",
			predicate: func() Predicate {
				var p Predicate = NewIntNotInPredicate([]int64{7, 8})
				return p
			}(),
			keptChunks: 0,
			keptPages:  0,
			keptValues: 0,
			writeData: func(w *parquet.Writer) { //nolint:all
				require.NoError(t, w.Write(&testInt{7}))
				require.NoError(t, w.Write(&testInt{8}))
			},
		},
	}

	for _, tC := range testCases {
		t.Run(tC.testName, func(t *testing.T) {
			testPredicate(t, tC)
		})
	}
}

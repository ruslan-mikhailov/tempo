package traceql

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"math"
	"math/bits"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/grafana/tempo/pkg/tempopb"
	commonv1proto "github.com/grafana/tempo/pkg/tempopb/common/v1"
	"github.com/grafana/tempo/pkg/util"
	"github.com/prometheus/prometheus/model/labels"
)

const (
	internalLabelMetaType        = "__meta_type"
	internalMetaTypeCount        = "__count"
	internalLabelBucket          = "__bucket"
	internalLabelQueryFragment   = "_meta_query_fragment"
	maxExemplars                 = 100
	maxExemplarsPerBucket        = 2
	maxBatchFragments            = 64 // Limited by uint64 bitmap
	// NormalNaN is a quiet NaN. This is also math.NaN().
	normalNaN uint64 = 0x7ff8000000000001
)

// QueryFragment represents a single sub-query within a batch query.
// Used by CompileBatchMetricsQueryRange to process multiple queries in a single pass.
type QueryFragment struct {
	// ID uniquely identifies this fragment (typically the query string)
	ID string

	// Index is the bit position in the MatchedGroups bitmap (0-63)
	Index int

	// eval is the compiled pipeline evaluation function that filters spans.
	// This is Pipeline.evaluate from Compile(), which runs the query's
	// SpansetFilter to determine which spans match this fragment.
	eval SpansetFilterFunc

	// pipeline is the metrics aggregation pipeline for this fragment
	pipeline firstStageElement

	// storageReq contains the original conditions for this fragment
	storageReq *FetchSpansRequest
}

func DefaultQueryRangeStep(start, end uint64) uint64 {
	delta := time.Duration(end - start)

	// Try to get this many data points
	// Our baseline is is 1 hour @ 15s intervals
	baseline := delta / 240

	// Round down in intervals of 5s
	interval := baseline / (5 * time.Second) * (5 * time.Second)

	if interval < 5*time.Second {
		// Round down in intervals of 1s
		interval = baseline / time.Second * time.Second
	}

	if interval < time.Second {
		return uint64(time.Second.Nanoseconds())
	}

	return uint64(interval.Nanoseconds())
}

// TrimToBlockOverlap returns the overlap between the given time range and block.  It is used to
// split a block to only the portion overlapping, or when the entire block is within range then
// opportunistically remove time slots that are known to be unused.
// When possible and not exceeding the request range, borders are aligned to the step.
// When a block is split then the borders will maintain the nanosecond precision of the request.
func TrimToBlockOverlap(req *tempopb.QueryRangeRequest, blockStart, blockEnd time.Time) (uint64, uint64, uint64) {
	start := req.Start
	end := req.End
	step := req.Step

	// We subtract 1 nanosecond from the block's start time
	// to make sure we include the left border of the block.
	start2 := uint64(blockStart.UnixNano()) - 1
	// Block's endTime is rounded down to the nearest second and considered inclusive.
	// In order to include the right border with nanoseconds, we add 1 second
	blockEnd = blockEnd.Add(time.Second)
	end2 := uint64(blockEnd.UnixNano())

	// Align the block data range to the step. This is because
	// low-precision timestamps within the block may be rounded up to
	// this step.  If the boundaries exceed the overall range
	// they will get trimmed back down.
	start2 = alignStart(start2, end2, step, IsInstant(req))
	end2 = alignEnd(start2, end2, step, IsInstant(req))

	// Now trim to the overlap preserving nanosecond precision for
	// when we split a block.
	start = max(start, start2)
	end = min(end, end2)

	// if no instant flag but step is instant, restore it
	// this can happen during new version rollout
	if !req.HasInstant() && IsInstant(req) {
		step = end - start
	}

	// if had no instant flag set, had no instant step and become instant,
	// add one nanosecond. This usually happens to small blocks.
	wasAssumedRange := !req.HasInstant() && !IsInstant(req)
	willBecomeInstant := (end-start == step)
	if wasAssumedRange && willBecomeInstant {
		end++
	}

	return start, end, step
}

// TrimToBefore shortens the query window to only include before the given time.
// Request must be in unix nanoseconds already.
func TrimToBefore(req *tempopb.QueryRangeRequest, before time.Time) {
	wasInstant := IsInstant(req)
	beforeNs := uint64(before.UnixNano())

	req.Start = min(req.Start, beforeNs)
	req.End = min(req.End, beforeNs)
	// if has no instant query indicator param, restore step
	if !req.HasInstant() && wasInstant && !IsInstant(req) {
		req.Step = req.End - req.Start
	}
}

// TrimToAfter shortens the query window to only include after the given time.
// Request must be in unix nanoseconds already.
func TrimToAfter(req *tempopb.QueryRangeRequest, before time.Time) {
	wasInstant := IsInstant(req)
	beforeNs := uint64(before.UnixNano())

	req.Start = max(req.Start, beforeNs)
	req.End = max(req.End, beforeNs)
	// if has no instant query indicator param, restore step
	if !req.HasInstant() && wasInstant && !IsInstant(req) {
		req.Step = req.End - req.Start
	}
}

// IsInstant returns true if the request is an instant query. It relies on the instant flag.
// If the flag is not provided, it falls back on determining by step and range.
// The fallback logic is to provide consistent results during rollout of the instant flag
// and will be removed in a future release.
func IsInstant(req *tempopb.QueryRangeRequest) bool {
	if req == nil {
		return false
	}
	if req.HasInstant() {
		return req.GetInstant()
	}
	// false or no field defined, fallback to old logic.
	return req.End-req.Start == req.Step
}

// AlignRequest shifts the start and end times of the request to align with the step
// interval.  This gives more consistent results across refreshes of queries like "last 1 hour".
// Without alignment each refresh is shifted by seconds or even milliseconds and the time series
// calculations are sublty different each time. It's not wrong, but less preferred behavior.
func AlignRequest(req *tempopb.QueryRangeRequest) {
	if IsInstant(req) {
		return
	}

	// It doesn't really matter but the request fields are expected to be in nanoseconds.
	req.Start = alignStart(req.Start, req.End, req.Step, false)
	req.End = alignEnd(req.Start, req.End, req.Step, false)

	if req.Start > req.Step { // to avoid overflow
		req.Start -= req.Step // force to have additional initial bucket
	}
}

func AlignEndToLeft(req *tempopb.QueryRangeRequest) {
	if IsInstant(req) {
		return
	}

	if req.Step == 0 {
		return
	}

	mod := req.End % req.Step
	req.End -= mod
}

// Start time is rounded down to next step
func alignStart(start, _, step uint64, instant bool) uint64 {
	if step == 0 {
		return 0
	}
	if instant {
		return start
	}

	return start - start%step
}

// End time is rounded up to next step
func alignEnd(_, end, step uint64, instant bool) uint64 {
	if step == 0 {
		return 0
	}
	if instant {
		return end
	}

	mod := end % step
	if mod == 0 {
		return end
	}

	return end + (step - mod)
}

type Label struct {
	Name  string
	Value Static
}

type Labels []Label

func LabelsFromProto(ls []commonv1proto.KeyValue) Labels {
	out := make(Labels, 0, len(ls))
	for _, l := range ls {
		out = append(out, Label{Name: l.Key, Value: StaticFromAnyValue(l.Value)})
	}
	return out
}

func LabelsFromArgs(args ...any) Labels {
	if len(args)%2 != 0 {
		panic("LabelsFromArgs must be called with an even number of arguments")
	}

	out := make(Labels, 0, len(args)/2)

	for i := 0; i < len(args); i += 2 {
		name := args[i].(string)
		var val Static
		switch arg := args[i+1].(type) {
		case string:
			val = NewStaticString(arg)
		case int:
			val = NewStaticInt(arg)
		case float64:
			val = NewStaticFloat(arg)
		default:
			panic(fmt.Sprintf("LabelsFromArgs type not yet supported: %T", arg))

		}

		out = append(out, Label{Name: name, Value: val})
	}

	return out
}

func (ls Labels) Has(name string) bool {
	for _, l := range ls {
		if l.Name == name {
			return true
		}
	}
	return false
}

// String returns the prometheus-formatted version of the labels. Which is downcasting
// the typed TraceQL values to strings, with some special casing.
func (ls Labels) String() string {
	promLabels := labels.NewBuilder(labels.EmptyLabels())
	for _, l := range ls {
		var promValue string
		switch l.Value.Type {
		case TypeNil:
			promValue = "<nil>"
		case TypeString:
			s := l.Value.EncodeToString(false)
			switch s {
			case "nil":
				promValue = "<nil>"
			case "":
				promValue = "<empty>"
			default:
				promValue = s
			}
		case TypeInt:
			promValue = "int(" + l.Value.EncodeToString(false) + ")"
		default:
			promValue = l.Value.EncodeToString(false)
		}
		promLabels.Set(l.Name, promValue)
	}

	return promLabels.Labels().String()
}

func (ls Labels) MapKey() SeriesMapKey {
	key := SeriesMapKey{}
	for i := range ls {
		key[i] = SeriesMapLabel{Name: ls[i].Name, Value: ls[i].Value.MapKey()}
	}
	return key
}

// Add returns a copy of the labels with the given label added to the end.
func (ls Labels) Add(label Label) Labels {
	cp := make(Labels, len(ls)+1)
	copy(cp, ls)
	cp[len(cp)-1] = label
	return cp
}

type Exemplar struct {
	Labels      Labels
	Value       float64
	TimestampMs uint64
}

type TimeSeries struct {
	Labels    Labels
	Values    []float64
	Exemplars []Exemplar
}

type SeriesMapLabel struct {
	Name  string
	Value StaticMapKey
}

type SeriesMapKey [maxGroupBys]SeriesMapLabel

// SeriesSet is a set of unique timeseries. They are mapped by the "Prometheus"-style
// text description: {x="a",y="b"} for convenience.
type SeriesSet map[SeriesMapKey]TimeSeries

func (set SeriesSet) ToProto(req *tempopb.QueryRangeRequest) []*tempopb.TimeSeries {
	mapper := NewIntervalMapperFromReq(req)
	resp := make([]*tempopb.TimeSeries, 0, len(set))

	for _, s := range set {
		labels := make([]commonv1proto.KeyValue, 0, len(s.Labels))
		for _, label := range s.Labels {
			labels = append(labels,
				commonv1proto.KeyValue{
					Key:   label.Name,
					Value: label.Value.AsAnyValue(),
				},
			)
		}

		intervals := mapper.IntervalCount()
		samples := make([]tempopb.Sample, 0, intervals)
		for i, value := range s.Values {
			// todo: this loop should be able to be restructured to directly pass over
			// the desired intervals
			if i >= intervals || math.IsNaN(value) {
				continue
			}

			ts := mapper.TimestampOf(i)
			samples = append(samples, tempopb.Sample{
				TimestampMs: time.Unix(0, int64(ts)).UnixMilli(),
				Value:       value,
			})
		}
		// Do not include empty TimeSeries
		if len(samples) == 0 {
			continue
		}

		var exemplars []tempopb.Exemplar
		if len(s.Exemplars) > 0 {
			exemplars = make([]tempopb.Exemplar, 0, len(s.Exemplars))
		}
		for _, e := range s.Exemplars {
			// skip exemplars that has NaN value
			i := mapper.IntervalMs(int64(e.TimestampMs))
			if i < 0 || i >= len(s.Values) || math.IsNaN(s.Values[i]) { // strict bounds check
				continue
			}

			labels := make([]commonv1proto.KeyValue, 0, len(e.Labels))
			for _, label := range e.Labels {
				labels = append(labels,
					commonv1proto.KeyValue{
						Key:   label.Name,
						Value: label.Value.AsAnyValue(),
					},
				)
			}

			exemplars = append(exemplars, tempopb.Exemplar{
				Labels:      labels,
				Value:       e.Value,
				TimestampMs: int64(e.TimestampMs),
			})
		}

		ss := &tempopb.TimeSeries{
			Labels:    labels,
			Samples:   samples,
			Exemplars: exemplars,
		}

		resp = append(resp, ss)
	}

	return resp
}

// VectorAggregator turns a vector of spans into a single numeric scalar
type VectorAggregator interface {
	Observe(s Span)
	Sample() float64
}

// RangeAggregator sorts spans into time slots
// TODO - for efficiency we probably combine this with VectorAggregator (see todo about CountOverTimeAggregator)
type RangeAggregator interface {
	Observe(s Span)
	ObserveExemplar(float64, uint64, Labels)
	Samples() []float64
	Exemplars() []Exemplar
	Length() int
}

// SpanAggregator sorts spans into series
type SpanAggregator interface {
	Observe(Span)
	ObserveExemplar(Span, float64, uint64)
	Series() SeriesSet
	Length() int
}

// CountOverTimeAggregator counts the number of spans. It can also
// calculate the rate when given a multiplier.
// TODO - Rewrite me to be []float64 which is more efficient
type CountOverTimeAggregator struct {
	count    float64
	rateMult float64
}

var _ VectorAggregator = (*CountOverTimeAggregator)(nil)

func NewCountOverTimeAggregator() *CountOverTimeAggregator {
	return &CountOverTimeAggregator{
		rateMult: 1.0,
	}
}

func NewRateAggregator(rateMult float64) *CountOverTimeAggregator {
	return &CountOverTimeAggregator{
		rateMult: rateMult,
	}
}

func (c *CountOverTimeAggregator) Observe(_ Span) {
	c.count++
}

func (c *CountOverTimeAggregator) Sample() float64 {
	return c.count * c.rateMult
}

// MinOverTimeAggregator it calculates the mininum value over time. It can also
// calculate the rate when given a multiplier.
type OverTimeAggregator struct {
	getSpanAttValue func(s Span) float64
	agg             func(current, n float64) float64
	val             float64
}

var _ VectorAggregator = (*OverTimeAggregator)(nil)

func NewOverTimeAggregator(attr Attribute, op SimpleAggregationOp) *OverTimeAggregator {
	var fn func(s Span) float64
	var agg func(current, n float64) float64

	switch op {
	case maxOverTimeAggregation:
		agg = maxOverTime()
	case minOverTimeAggregation:
		agg = minOverTime()
	case sumOverTimeAggregation:
		agg = sumOverTime()
	}

	switch attr {
	case IntrinsicDurationAttribute:
		fn = func(s Span) float64 {
			return float64(s.DurationNanos()) / float64(time.Second)
		}
	default:
		fn = func(s Span) float64 {
			f, a := FloatizeAttribute(s, attr)
			if a == TypeNil {
				return math.Float64frombits(normalNaN)
			}
			return f
		}
	}

	return &OverTimeAggregator{
		getSpanAttValue: fn,
		agg:             agg,
		val:             math.Float64frombits(normalNaN),
	}
}

func (c *OverTimeAggregator) Observe(s Span) {
	c.val = c.agg(c.val, c.getSpanAttValue(s))
}

func (c *OverTimeAggregator) Sample() float64 {
	return c.val
}

// StepAggregator sorts spans into time slots using a step interval like 30s or 1m
type StepAggregator struct {
	intervalMapper  IntervalMapper
	vectors         []VectorAggregator
	exemplars       []Exemplar
	exemplarBuckets bucketSet
}

var _ RangeAggregator = (*StepAggregator)(nil)

func NewStepAggregator(start, end, step uint64, innerAgg func() VectorAggregator) *StepAggregator {
	const instant = false // never used for instant queries
	mapper := NewIntervalMapper(start, end, step, instant)
	intervals := mapper.IntervalCount()
	vectors := make([]VectorAggregator, intervals)
	for i := range vectors {
		vectors[i] = innerAgg()
	}

	return &StepAggregator{
		intervalMapper:  mapper,
		vectors:         vectors,
		exemplars:       make([]Exemplar, 0, maxExemplars),
		exemplarBuckets: newExemplarBucketSet(maxExemplars, start, end, step, instant),
	}
}

func (s *StepAggregator) Observe(span Span) {
	interval := s.intervalMapper.Interval(span.StartTimeUnixNanos())
	if interval == -1 {
		return
	}
	s.vectors[interval].Observe(span)
}

func (s *StepAggregator) ObserveExemplar(value float64, ts uint64, lbls Labels) {
	if s.exemplarBuckets.testTotal() {
		return
	}
	if s.exemplarBuckets.addAndTest(ts) {
		return
	}

	s.exemplars = append(s.exemplars, Exemplar{
		Labels:      lbls,
		Value:       value,
		TimestampMs: ts,
	})
}

func (s *StepAggregator) Samples() []float64 {
	ss := make([]float64, len(s.vectors))
	for i, v := range s.vectors {
		ss[i] = v.Sample()
	}
	return ss
}

func (s *StepAggregator) Exemplars() []Exemplar {
	return s.exemplars
}

func (s *StepAggregator) Length() int {
	return 0
}

// InstantAggregator is similar to StepAggregator but always has only one interval.
// It's used for instant queries where we only need a single data point.
type InstantAggregator struct {
	start, end uint64
	vector     VectorAggregator
}

var _ RangeAggregator = (*InstantAggregator)(nil)

func NewInstantAggregator(start, end uint64, innerAgg func() VectorAggregator) *InstantAggregator {
	return &InstantAggregator{
		start:  start,
		end:    end,
		vector: innerAgg(),
	}
}

func (i *InstantAggregator) Observe(span Span) {
	// if outside the range, skip
	if ts := span.StartTimeUnixNanos(); ts < i.start || ts > i.end {
		return
	}
	i.vector.Observe(span)
}

func (i *InstantAggregator) ObserveExemplar(float64, uint64, Labels) {
	// instant query does not have exemplars, so we skip
}

func (i *InstantAggregator) Samples() []float64 {
	return []float64{i.vector.Sample()}
}

func (i *InstantAggregator) Exemplars() []Exemplar {
	return nil
}

func (i *InstantAggregator) Length() int {
	return 0
}

const maxGroupBys = 5 // TODO - This isn't ideal but see comment below.

// FastValues is an array of attribute values (static values) that can be used
// as a map key.  This offers good performance and works with native Go maps and
// has no chance for collisions (whereas a hash32 has a non-zero chance of
// collisions).  However, it means we have to arbitrarily set an upper limit on
// the maximum number of values.

type (
	FastStatic1 [1]StaticMapKey
	FastStatic2 [2]StaticMapKey
	FastStatic3 [3]StaticMapKey
	FastStatic4 [4]StaticMapKey
	FastStatic5 [5]StaticMapKey
)

type FastStatic interface {
	FastStatic1 | FastStatic2 | FastStatic3 | FastStatic4 | FastStatic5
}

type (
	StaticVals1 [1]Static
	StaticVals2 [2]Static
	StaticVals3 [3]Static
	StaticVals4 [4]Static
	StaticVals5 [5]Static
)

type StaticVals interface {
	StaticVals1 | StaticVals2 | StaticVals3 | StaticVals4 | StaticVals5
}

// GroupingAggregator groups spans into series based on attribute values.
type GroupingAggregator[F FastStatic, S StaticVals] struct {
	// Config
	by          []Attribute               // Original attributes: .foo
	byLookups   [][]Attribute             // Lookups: span.foo resource.foo
	byFunc      func(Span) (Static, bool) // Dynamic label calculated by a callback
	byFuncLabel string                    // Name of the dynamic label
	innerAgg    func() RangeAggregator

	// Data
	series     map[F]aggregatorWitValues[S]
	lastSeries aggregatorWitValues[S]
	buf        fastStaticWithValues[F, S]
	lastBuf    fastStaticWithValues[F, S]
}

type aggregatorWitValues[S StaticVals] struct {
	agg  RangeAggregator
	vals S
}

type fastStaticWithValues[F FastStatic, S StaticVals] struct {
	fast F
	vals S
}

var _ SpanAggregator = (*GroupingAggregator[FastStatic1, StaticVals1])(nil)

func NewGroupingAggregator(aggName string, innerAgg func() RangeAggregator, by []Attribute, byFunc func(Span) (Static, bool), byFuncLabel string) SpanAggregator {
	if len(by) == 0 && byFunc == nil {
		return &UngroupedAggregator{
			name:     aggName,
			innerAgg: innerAgg(),
		}
	}

	lookups := make([][]Attribute, len(by))
	for i, attr := range by {
		if attr.Intrinsic == IntrinsicNone && attr.Scope == AttributeScopeNone {
			// Unscoped attribute. Check span-level, then resource-level.
			// TODO - Is this taken care of by span.AttributeFor now?
			lookups[i] = []Attribute{
				NewScopedAttribute(AttributeScopeSpan, false, attr.Name),
				NewScopedAttribute(AttributeScopeResource, false, attr.Name),
			}
		} else {
			lookups[i] = []Attribute{attr}
		}
	}

	aggNum := len(lookups)
	if byFunc != nil {
		aggNum++
	}

	switch aggNum {
	case 1:
		return newGroupingAggregator[FastStatic1, StaticVals1](innerAgg, by, byFunc, byFuncLabel, lookups)
	case 2:
		return newGroupingAggregator[FastStatic2, StaticVals2](innerAgg, by, byFunc, byFuncLabel, lookups)
	case 3:
		return newGroupingAggregator[FastStatic3, StaticVals3](innerAgg, by, byFunc, byFuncLabel, lookups)
	case 4:
		return newGroupingAggregator[FastStatic4, StaticVals4](innerAgg, by, byFunc, byFuncLabel, lookups)
	case 5:
		return newGroupingAggregator[FastStatic5, StaticVals5](innerAgg, by, byFunc, byFuncLabel, lookups)
	default:
		panic("unsupported number of group-bys")
	}
}

func newGroupingAggregator[F FastStatic, S StaticVals](innerAgg func() RangeAggregator, by []Attribute, byFunc func(Span) (Static, bool), byFuncLabel string, lookups [][]Attribute) SpanAggregator {
	return &GroupingAggregator[F, S]{
		series:      map[F]aggregatorWitValues[S]{},
		by:          by,
		byFunc:      byFunc,
		byFuncLabel: byFuncLabel,
		byLookups:   lookups,
		innerAgg:    innerAgg,
	}
}

// getGroupingValues gets the grouping values for the span and stores them in the buffer.
// Returns false if the span should be dropped.
func (g *GroupingAggregator[F, S]) getGroupingValues(span Span) bool {
	// Get grouping values
	// Reuse same buffer
	// There is no need to reset, the number of group-by attributes
	// is fixed after creation.
	for i, lookups := range g.byLookups {
		val := lookup(lookups, span)
		g.buf.vals[i] = val
		g.buf.fast[i] = val.MapKey()
	}

	// If dynamic label exists calculate and append it
	if g.byFunc != nil {
		v, ok := g.byFunc(span)
		if !ok {
			// Totally drop this span
			return false
		}
		g.buf.vals[len(g.byLookups)] = v
		g.buf.fast[len(g.byLookups)] = v.MapKey()
	}

	return true
}

// getSeries gets the series for the current span.
// It will reuse the last series if possible.
func (g *GroupingAggregator[F, S]) getSeries() aggregatorWitValues[S] {
	// Fast path
	if g.lastSeries.agg != nil && g.lastBuf.fast == g.buf.fast {
		return g.lastSeries
	}

	s, ok := g.series[g.buf.fast]
	if !ok {
		s.agg = g.innerAgg()
		s.vals = g.buf.vals
		g.series[g.buf.fast] = s
	}

	g.lastBuf = g.buf
	g.lastSeries = s
	return s
}

// Observe the span by looking up its group-by attributes, mapping to the series,
// and passing to the inner aggregate.  This is a critical hot path.
func (g *GroupingAggregator[F, S]) Observe(span Span) {
	if !g.getGroupingValues(span) {
		return
	}

	s := g.getSeries()
	s.agg.Observe(span)
}

func (g *GroupingAggregator[F, S]) ObserveExemplar(span Span, value float64, ts uint64) {
	if !g.getGroupingValues(span) {
		return
	}

	s := g.getSeries()

	// Observe exemplar
	all := span.AllAttributes()
	lbls := make(Labels, 0, len(all))
	for k, v := range span.AllAttributes() {
		lbls = append(lbls, Label{k.String(), v})
	}
	s.agg.ObserveExemplar(value, ts, lbls)
}

func (g *GroupingAggregator[F, S]) Length() int {
	return len(g.series)
}

// labelsFor gives the final labels for the series. Slower and not on the hot path.
// This is tweaked to match what prometheus does where possible with an exception.
// In the case of all values missing.
// (1) Standard case: a label is created for each group-by value in the series:
//
//	Ex: rate() by (x,y) can yield:
//	{x=a,y=b}
//	{x=c,y=d}
//	etc
//
// (2) Nils are dropped. A nil can be present for any label, so any combination
// of the remaining labels is possible. Label order is preserved.
//
//	Ex: rate() by (x,y,z) can yield all of these combinations:
//	{x=..., y=..., z=...}
//	{x=...,        z=...}
//	{x=...              }
//	{       y=..., z=...}
//	{       y=...       }
//	etc
//
// (3) Exceptional case: All Nils. For the TraceQL data-type aware labels we still drop
// all nils which results in an empty label set. But Prometheus-style always have
// at least 1 label, so in that case we have to force at least 1 label or else things
// may not be handled correctly downstream.  In this case we take the first label and
// make it the string "nil"
//
//	Ex: rate() by (x,y,z) and all nil yields:
//	{x="nil"}
func (g *GroupingAggregator[F, S]) labelsFor(vals S) (Labels, SeriesMapKey) {
	labels := make(Labels, 0, len(g.by)+1)
	for i := range g.by {
		if vals[i].Type == TypeNil {
			continue
		}
		labels = append(labels, Label{g.by[i].String(), vals[i]})
	}
	if g.byFunc != nil {
		labels = append(labels, Label{g.byFuncLabel, vals[len(g.by)]})
	}

	if len(labels) == 0 {
		// When all nil then force one
		labels = append(labels, Label{g.by[0].String(), NewStaticNil()})
	}

	return labels, labels.MapKey()
}

func (g *GroupingAggregator[F, S]) Series() SeriesSet {
	ss := SeriesSet{}

	for _, s := range g.series {
		labels, key := g.labelsFor(s.vals)

		ss[key] = TimeSeries{
			Labels:    labels,
			Values:    s.agg.Samples(),
			Exemplars: s.agg.Exemplars(),
		}
	}

	return ss
}

// UngroupedAggregator builds a single series with no labels. e.g. {} | rate()
type UngroupedAggregator struct {
	name     string
	innerAgg RangeAggregator
}

var _ SpanAggregator = (*UngroupedAggregator)(nil)

func (u *UngroupedAggregator) Observe(span Span) {
	u.innerAgg.Observe(span)
}

func (u *UngroupedAggregator) ObserveExemplar(span Span, value float64, ts uint64) {
	all := span.AllAttributes()
	lbls := make(Labels, 0, len(all))
	for k, v := range all {
		lbls = append(lbls, Label{k.String(), v})
	}
	u.innerAgg.ObserveExemplar(value, ts, lbls)
}

func (u *UngroupedAggregator) Length() int {
	return 0
}

// Series output.
// This is tweaked to match what prometheus does.  For ungrouped metrics we
// fill in a placeholder metric name with the name of the aggregation.
// rate() => {__name__=rate}
func (u *UngroupedAggregator) Series() SeriesSet {
	labels := LabelsFromArgs(labels.MetricName, u.name)

	return SeriesSet{
		labels.MapKey(): {
			Labels:    labels,
			Values:    u.innerAgg.Samples(),
			Exemplars: u.innerAgg.Exemplars(),
		},
	}
}

func (e *Engine) CompileMetricsQueryRangeNonRaw(req *tempopb.QueryRangeRequest, mode AggregateMode) (*MetricsFrontendEvaluator, error) {
	if req.Start <= 0 {
		return nil, fmt.Errorf("start required")
	}
	if req.End <= 0 {
		return nil, fmt.Errorf("end required")
	}
	if req.End <= req.Start {
		return nil, fmt.Errorf("end must be greater than start")
	}
	if req.Step <= 0 {
		return nil, fmt.Errorf("step required")
	}

	expr, _, metricsPipeline, metricsSecondStage, _, err := Compile(req.Query)
	if err != nil {
		return nil, fmt.Errorf("compiling query: %w", err)
	}

	// MetricsMath expressions use per-fragment pipelines for L2/L3 aggregation.
	if expr.MetricsMath != nil {
		return e.compileMetricsMathNonRaw(req, expr, mode)
	}

	// for metrics queries, we need a metrics pipeline
	if metricsPipeline == nil {
		return nil, fmt.Errorf("not a metrics query")
	}

	metricsPipeline.init(req, mode)
	mfe := &MetricsFrontendEvaluator{
		metricsPipeline: metricsPipeline,
	}

	// only run metrics second stage if we have second stage and query mode = final,
	// as we are not sharding them now in lower layers.
	if metricsSecondStage != nil && mode == AggregateModeFinal {
		metricsSecondStage.init(req)
		mfe.metricsSecondStage = metricsSecondStage
	}

	return mfe, nil
}

// CompileMetricsQueryRange returns an evaluator that can be reused across multiple data sources.
// Dedupe spans parameter is an indicator of whether to expected duplicates in the datasource. For
// example if the datasource is replication factor=1 or only a single block then we know there
// aren't duplicates, and we can make some optimizations.
func (e *Engine) CompileMetricsQueryRange(req *tempopb.QueryRangeRequest, exemplars int, timeOverlapCutoff float64, allowUnsafeQueryHints bool) (*MetricsEvaluator, error) {
	if req.Start <= 0 {
		return nil, fmt.Errorf("start required")
	}
	if req.End <= 0 {
		return nil, fmt.Errorf("end required")
	}
	if req.End <= req.Start {
		return nil, fmt.Errorf("end must be greater than start")
	}
	if req.Step <= 0 {
		return nil, fmt.Errorf("step required")
	}

	expr, eval, metricsPipeline, _, storageReq, err := Compile(req.Query)
	if err != nil {
		return nil, fmt.Errorf("compiling query: %w", err)
	}

	// MetricsMath expressions (e.g. "({...} | rate()) / ({} | count_over_time())")
	// are compiled as batch fragments with merged conditions.
	if expr.MetricsMath != nil {
		return e.compileMetricsMathQueryRange(req, expr, exemplars, timeOverlapCutoff)
	}

	if metricsPipeline == nil {
		return nil, fmt.Errorf("not a metrics query")
	}

	// the exemplars hint supports both bool and int. first we test for the integer value. if
	// its not present then we look to see if the user provided `with(exemplars=false)`
	if v, ok := expr.Hints.GetInt(HintExemplars, allowUnsafeQueryHints); ok {
		exemplars = v
	} else if v, ok := expr.Hints.GetBool(HintExemplars, allowUnsafeQueryHints); ok {
		if !v {
			exemplars = 0
		}
	}

	// Debug sampling hints, remove once we settle on approach.
	if traceSample, traceSampleOk := expr.Hints.GetFloat(HintTraceSample, allowUnsafeQueryHints); traceSampleOk {
		storageReq.TraceSampler = newProbablisticSampler(traceSample)
	}
	if spanSample, spanSampleOk := expr.Hints.GetFloat(HintSpanSample, allowUnsafeQueryHints); spanSampleOk {
		storageReq.SpanSampler = newProbablisticSampler(spanSample)
	}

	if sample, sampleOk := expr.Hints.GetBool(HintSample, allowUnsafeQueryHints); sampleOk && sample {
		// Automatic sampling
		// Get other params
		s := newAdaptiveSampler()
		if debug, ok := expr.Hints.GetBool(HintDebug, allowUnsafeQueryHints); ok {
			s.debug = debug
		}
		if info, ok := expr.Hints.GetBool(HintInfo, allowUnsafeQueryHints); ok {
			s.info = info
		}

		// Classify the query and determine if it needs to be at the trace-level or can be at span-level (better)
		if expr.NeedsFullTrace() {
			storageReq.TraceSampler = s
		} else {
			storageReq.SpanSampler = s
		}
	}

	if sampleFraction, ok := expr.Hints.GetFloat(HintSample, allowUnsafeQueryHints); ok && sampleFraction > 0 && sampleFraction < 1 {
		// Fixed sampling rate.
		s := newProbablisticSampler(sampleFraction)

		// Classify the query and determine if it needs to be at the trace-level or can be at span-level (better)
		if expr.NeedsFullTrace() {
			storageReq.TraceSampler = s
		} else {
			storageReq.SpanSampler = s
		}
	}

	// This initializes all step buffers, counters, etc
	metricsPipeline.init(req, AggregateModeRaw)

	me := &MetricsEvaluator{
		storageReq:        storageReq,
		metricsPipeline:   metricsPipeline,
		timeOverlapCutoff: timeOverlapCutoff,
		maxExemplars:      exemplars,
		exemplarMap:       make(map[string]struct{}, exemplars), // TODO: Lazy, use bloom filter, CM sketch or something
	}

	// If the request range is fully aligned to the step, then we can use lower
	// precision data that matches the step while still returning accurate results.
	// When the range isn't an even multiple, it means that we are on the split
	// between backend and recent data, or the edges of the request. In that case
	// we use full nanosecond precision.
	var precision time.Duration
	if (req.Start%req.Step) == 0 && (req.End%req.Step) == 0 {
		precision = time.Duration(req.Step)
	}

	// Span start time (always required)
	if !storageReq.HasAttribute(IntrinsicSpanStartTimeAttribute) {
		// Technically we only need the start time of matching spans, so we add it to the second pass.
		// However this is often optimized back to the first pass when it lets us avoid a second pass altogether.
		storageReq.SecondPassConditions = append(storageReq.SecondPassConditions, Condition{Attribute: IntrinsicSpanStartTimeAttribute, Precision: precision})
	} else {
		// Update the existing condition to use low precision
		for i, c := range storageReq.Conditions {
			if c.Attribute == IntrinsicSpanStartTimeAttribute {
				storageReq.Conditions[i].Precision = precision
				break
			}
		}
	}

	// Timestamp filtering
	// (1) Include any overlapping trace
	//     It can be faster to skip the trace-level timestamp check
	//     when all or most of the traces overlap the window.
	//     So this is done dynamically on a per-fetcher basis in Do()
	// (2) Only include spans that started in this time frame.
	//     This is checked outside the fetch layer in the evaluator. Timestamp
	//     is only checked on the spans that are the final results.
	// TODO - I think there are cases where we can push this down.
	// Queries like {status=error} | rate() don't assert inter-span conditions
	// and we could filter on span start time without affecting correctness.
	// Queries where we can't are like:  {A} >> {B} | rate() because only require
	// that {B} occurs within our time range but {A} is allowed to occur any time.
	me.checkTime = true
	me.start = req.Start
	me.end = req.End

	if me.maxExemplars > 0 {
		cb := func() bool { return me.exemplarCount < me.maxExemplars }
		meta := ExemplarMetaConditionsWithout(cb, storageReq.SecondPassConditions, storageReq.AllConditions)
		storageReq.SecondPassConditions = append(storageReq.SecondPassConditions, meta...)
	}
	// Setup second pass callback.  It might be optimized away
	storageReq.SecondPass = func(s *Spanset) ([]*Spanset, error) {
		// The traceql engine isn't thread-safe.
		// But parallelization is required for good metrics performance.
		// So we do external locking here.
		me.mtx.Lock()
		defer me.mtx.Unlock()
		return eval([]*Spanset{s})
	}

	optimize(storageReq)

	return me, nil
}

// collectLeafMetrics traverses a MetricsMathOp tree and returns all leaf
// RootExpr nodes (those with MetricsPipeline set) in left-to-right order.
// For an expression like (A / B) + C, it returns [A, B, C].
func collectLeafMetrics(r *RootExpr) []*RootExpr {
	if r.MetricsMath != nil {
		return append(collectLeafMetrics(r.MetricsMath.LHS), collectLeafMetrics(r.MetricsMath.RHS)...)
	}
	if r.MetricsPipeline != nil {
		return []*RootExpr{r}
	}
	return nil
}

// compileMetricsMathQueryRange compiles a MetricsMath expression into a batch
// MetricsEvaluator. It extracts leaf metrics queries from the math tree, creates
// a fragment for each leaf, and merges their conditions for efficient storage access.
//
// This produces the same fragment-labeled output as CompileBatchMetricsQueryRange
// but works from the parsed AST rather than separate query strings.
func (e *Engine) compileMetricsMathQueryRange(
	req *tempopb.QueryRangeRequest,
	expr *RootExpr,
	exemplars int,
	timeOverlapCutoff float64,
) (*MetricsEvaluator, error) {
	leaves := collectLeafMetrics(expr)
	if len(leaves) == 0 {
		return nil, fmt.Errorf("no metrics queries found in expression")
	}
	if len(leaves) > maxBatchFragments {
		return nil, fmt.Errorf("too many sub-queries: %d exceeds maximum %d", len(leaves), maxBatchFragments)
	}

	fragments := make([]QueryFragment, len(leaves))
	for i, leaf := range leaves {
		leafReq := &FetchSpansRequest{AllConditions: true}
		leaf.extractConditions(leafReq)

		leaf.MetricsPipeline.init(req, AggregateModeRaw)

		fragments[i] = QueryFragment{
			ID:         leaf.String(),
			Index:      i,
			eval:       leaf.Pipeline.evaluate,
			pipeline:   leaf.MetricsPipeline,
			storageReq: leafReq,
		}
	}

	mergedReq := mergeFragmentConditions(fragments)

	me := &MetricsEvaluator{
		storageReq:        mergedReq,
		fragments:         fragments,
		timeOverlapCutoff: timeOverlapCutoff,
		maxExemplars:      exemplars,
		exemplarMap:       make(map[string]struct{}, exemplars),
		start:             req.Start,
		end:               req.End,
		checkTime:         true,
	}

	// Add span start time for time filtering in Do()
	if !mergedReq.HasAttribute(IntrinsicSpanStartTimeAttribute) {
		mergedReq.SecondPassConditions = append(mergedReq.SecondPassConditions,
			Condition{Attribute: IntrinsicSpanStartTimeAttribute})
	}

	return me, nil
}

// compileMetricsMathNonRaw compiles a MetricsMath expression for L2/L3 aggregation.
// It extracts leaf metrics queries, creates a per-fragment pipeline for each,
// and stores the math tree for Final mode evaluation.
func (e *Engine) compileMetricsMathNonRaw(
	req *tempopb.QueryRangeRequest,
	expr *RootExpr,
	mode AggregateMode,
) (*MetricsFrontendEvaluator, error) {
	leaves := collectLeafMetrics(expr)
	if len(leaves) == 0 {
		return nil, fmt.Errorf("no metrics queries found in expression")
	}

	fragmentPipelines := make(map[string]firstStageElement, len(leaves))
	for _, leaf := range leaves {
		leaf.MetricsPipeline.init(req, mode)
		fragmentPipelines[leaf.String()] = leaf.MetricsPipeline
	}

	mfe := &MetricsFrontendEvaluator{
		fragmentPipelines: fragmentPipelines,
	}

	if mode == AggregateModeFinal {
		mfe.metricsMath = expr.MetricsMath
	}

	return mfe, nil
}

// stripFragmentLabel reads and removes the _meta_query_fragment label from a proto
// TimeSeries. Returns the fragment ID and a new TimeSeries that shares the original's
// Samples and Exemplars slices. Returns ("", nil) if the label is absent.
func stripFragmentLabel(ts *tempopb.TimeSeries) (string, *tempopb.TimeSeries) {
	var fragmentID string
	labels := make([]commonv1proto.KeyValue, 0, len(ts.Labels))
	for _, l := range ts.Labels {
		if l.Key == internalLabelQueryFragment {
			fragmentID = l.Value.GetStringValue()
			continue
		}
		labels = append(labels, l)
	}
	if fragmentID == "" {
		return "", nil
	}
	return fragmentID, &tempopb.TimeSeries{
		Labels:    labels,
		Samples:   ts.Samples,
		Exemplars: ts.Exemplars,
	}
}

// evaluateMetricsMathOp recursively evaluates a MetricsMathOp tree using fragment results.
func evaluateMetricsMathOp(math *MetricsMathOp, fragmentResults map[string]SeriesSet) SeriesSet {
	lhs := evaluateMetricsMathExpr(math.LHS, fragmentResults)
	rhs := evaluateMetricsMathExpr(math.RHS, fragmentResults)
	return applySeriesSetOp(math.Op, lhs, rhs)
}

// evaluateMetricsMathExpr returns the SeriesSet for a RootExpr node in the math tree.
// For leaves (MetricsMath == nil), returns the fragment's results.
// For nested math (MetricsMath != nil), recurses via evaluateMetricsMathOp.
func evaluateMetricsMathExpr(expr *RootExpr, fragmentResults map[string]SeriesSet) SeriesSet {
	if expr.MetricsMath != nil {
		return evaluateMetricsMathOp(expr.MetricsMath, fragmentResults)
	}
	return fragmentResults[expr.String()]
}

// applySeriesSetOp performs a binary math operation on two SeriesSets using inner join.
// Series are matched by SeriesMapKey (labels). Only series present in both LHS and RHS
// produce output. Values are combined element-wise using the given operator.
func applySeriesSetOp(op Operator, lhs, rhs SeriesSet) SeriesSet {
	result := make(SeriesSet, len(lhs))

	for key, lhsTS := range lhs {
		rhsTS, ok := rhs[key]
		if !ok {
			continue // No matching series in RHS (inner join)
		}

		// Apply operation element-wise on Values
		n := len(lhsTS.Values)
		if len(rhsTS.Values) < n {
			n = len(rhsTS.Values)
		}
		values := make([]float64, n)
		for i := 0; i < n; i++ {
			values[i] = applyMetricsOp(op, lhsTS.Values[i], rhsTS.Values[i])
		}

		result[key] = TimeSeries{
			Labels: lhsTS.Labels,
			Values: values,
		}
	}

	return result
}

// applyMetricsOp applies a single arithmetic operation on two float64 values.
// Division by zero returns NaN. Go's native NaN propagation handles NaN inputs.
func applyMetricsOp(op Operator, a, b float64) float64 {
	switch op {
	case OpAdd:
		return a + b
	case OpSub:
		return a - b
	case OpMult:
		return a * b
	case OpDiv:
		if b == 0 {
			return math.NaN()
		}
		return a / b
	default:
		return math.NaN()
	}
}

// CompileBatchMetricsQueryRange compiles multiple metrics queries for batch processing.
// All queries are executed in a single pass through the data. Results are labeled with
// _meta_query_fragment to identify their source query.
//
// This is more efficient than running separate queries because:
// 1. Single pass through storage (conditions merged with OR logic)
// 2. Each span is observed by all fragment pipelines
//
// Limitations:
//   - Maximum 64 fragments (limited by uint64 bitmap)
func (e *Engine) CompileBatchMetricsQueryRange(
	req *tempopb.QueryRangeRequest,
	queries []string,
	exemplars int,
	timeOverlapCutoff float64,
	allowUnsafeQueryHints bool,
) (*MetricsEvaluator, error) {
	// Validate request parameters
	if req.Start <= 0 {
		return nil, fmt.Errorf("start required")
	}
	if req.End <= 0 {
		return nil, fmt.Errorf("end required")
	}
	if req.End <= req.Start {
		return nil, fmt.Errorf("end must be greater than start")
	}
	if req.Step <= 0 {
		return nil, fmt.Errorf("step required")
	}

	// Validate fragment count
	if len(queries) == 0 {
		return nil, fmt.Errorf("at least one query required")
	}
	if len(queries) > maxBatchFragments {
		return nil, fmt.Errorf("too many queries: %d exceeds maximum %d", len(queries), maxBatchFragments)
	}

	// Compile each query and validate structure
	fragments := make([]QueryFragment, len(queries))
	for i, q := range queries {
		_, eval, metricsPipeline, _, storageReq, err := Compile(q)
		if err != nil {
			return nil, fmt.Errorf("compiling query %d (%q): %w", i, q, err)
		}
		if metricsPipeline == nil {
			return nil, fmt.Errorf("query %d (%q) is not a metrics query", i, q)
		}

		metricsPipeline.init(req, AggregateModeRaw)

		fragments[i] = QueryFragment{
			ID:         q,
			Index:      i,
			eval:       eval,
			pipeline:   metricsPipeline,
			storageReq: storageReq,
		}
	}

	// Merge conditions from all fragments
	mergedReq := mergeFragmentConditions(fragments)

	// Create evaluator
	me := &MetricsEvaluator{
		storageReq:        mergedReq,
		fragments:         fragments,
		timeOverlapCutoff: timeOverlapCutoff,
		maxExemplars:      exemplars,
		exemplarMap:       make(map[string]struct{}, exemplars),
		start:             req.Start,
		end:               req.End,
		checkTime:         true,
	}

	// Add required conditions for time filtering
	if !mergedReq.HasAttribute(IntrinsicSpanStartTimeAttribute) {
		mergedReq.SecondPassConditions = append(mergedReq.SecondPassConditions,
			Condition{Attribute: IntrinsicSpanStartTimeAttribute})
	}

	return me, nil
}

// mergeFragmentConditions merges conditions from all fragments into a single FetchSpansRequest.
// Uses OR logic (AllConditions=false) to fetch spans matching any fragment.
func mergeFragmentConditions(fragments []QueryFragment) *FetchSpansRequest {
	merged := &FetchSpansRequest{
		AllConditions: false, // OR logic - match any fragment
	}

	conditionKey := func(c Condition) string {
		// Create unique key for deduplication
		return fmt.Sprintf("%s:%v:%v", c.Attribute.String(), c.Op, c.Operands)
	}

	seenFirst := make(map[string]struct{})
	seenSecond := make(map[string]struct{})

	for _, f := range fragments {
		for _, c := range f.storageReq.Conditions {
			key := conditionKey(c)
			if _, ok := seenFirst[key]; !ok {
				seenFirst[key] = struct{}{}
				merged.Conditions = append(merged.Conditions, c)
			}
		}
		for _, c := range f.storageReq.SecondPassConditions {
			key := conditionKey(c)
			if _, ok := seenSecond[key]; !ok {
				seenSecond[key] = struct{}{}
				merged.SecondPassConditions = append(merged.SecondPassConditions, c)
			}
		}
	}

	return merged
}

// optimize numerous things within the request that is specific to metrics.
func optimize(req *FetchSpansRequest) {
	if !req.AllConditions || req.SecondPassSelectAll {
		return
	}

	// Unscoped attributes like .foo require the second pass to evaluate
	for _, c := range req.Conditions {
		if c.Attribute.Scope == AttributeScopeNone && c.Attribute.Intrinsic == IntrinsicNone {
			// Unscoped (non-intrinsic) attribute
			return
		}
	}

	// There is an issue where multiple conditions &&'ed on the same
	// attribute can look like AllConditions==true, but are implemented
	// in the storage layer like ||'ed and require the second pass callback (engine).
	// TODO(mdisibio) - This would be a big performance improvement if we can fix the storage layer
	// Example:
	//   { span.http.status_code >= 500 && span.http.status_code < 600 } | rate() by (span.http.status_code)
	exists := make(map[Attribute]struct{}, len(req.Conditions))
	for _, c := range req.Conditions {
		if _, ok := exists[c.Attribute]; ok {
			// Don't optimize
			return
		}
		exists[c.Attribute] = struct{}{}
	}

	// Special optimization for queries like:
	//  {} | rate()
	//  {} | rate() by (rootName)
	//  {} | rate() by (resource.service.name)
	// When the second pass consists only of intrinsics, then it's possible to
	// move them to the first pass and increase performance. It avoids the second pass/bridge
	// layer and doesn't alter the correctness of the query.
	// This can't be done for plain attributes or in all cases.
	if len(req.SecondPassConditions) > 0 {
		if !allIntrinsics(req.SecondPassConditions) {
			// Cannot move them.
			return
		}

		// Move all to first pass
		req.Conditions = append(req.Conditions, req.SecondPassConditions...)
		req.SecondPassConditions = nil
	}

	// If the remaining first layer is a single condition, then we can
	// eliminate the callback.
	if len(req.Conditions) == 1 {
		req.SecondPass = nil
		return
	}

	// Finally if the entire remaining first layer is intrinsics, then we can
	// elmininate the callback.
	// TODO(mdisibio): We can't eliminate the callback if other conditions exist, despite AllConditions, because the storage layer
	// doesn't strictly honor AllConditions for multiple attributes of the same type in the shared columns.
	// For example: { span.string_a = "foo" && span.string_b = "bar" } | rate()
	// This query requires the callback to assert the attributes correctly.
	// Dedicated columns aren't affected but we don't have the information to distinguish that here.
	if len(req.Conditions) > 0 && allIntrinsics(req.Conditions) {
		req.SecondPass = nil
	}
}

func allIntrinsics(conds []Condition) bool {
	for _, cond := range conds {
		if cond.Attribute.Intrinsic != IntrinsicNone {
			continue
		}

		// This is a very special case. resource.service.name is also always present
		// (required by spec) so it can be moved too.
		if cond.Attribute.Scope == AttributeScopeResource && cond.Attribute.Name == "service.name" {
			continue
		}

		// Anything else is not an intrinsic
		return false
	}
	return true
}

func lookup(needles []Attribute, haystack Span) Static {
	for _, n := range needles {
		if v, ok := haystack.AttributeFor(n); ok {
			return v
		}
	}

	return NewStaticNil()
}

type MetricsEvaluator struct {
	start, end                      uint64
	checkTime                       bool
	maxExemplars, exemplarCount     int
	exemplarMap                     map[string]struct{}
	timeOverlapCutoff               float64
	storageReq                      *FetchSpansRequest
	metricsPipeline                 firstStageElement
	spansTotal, spansDeduped, bytes uint64
	mtx                             sync.Mutex

	// fragments holds multiple query fragments for batch processing.
	// When nil or empty, single-query mode is used (backward compatible).
	fragments []QueryFragment
}

func timeRangeOverlap(reqStart, reqEnd, dataStart, dataEnd uint64) float64 {
	st := max(reqStart, dataStart)
	end := min(reqEnd, dataEnd)

	if end <= st {
		return 0
	}

	return float64(end-st) / float64(dataEnd-dataStart)
}

// Do metrics on the given source of data and merge the results into the working set.  Optionally, if provided,
// uses the known time range of the data for last-minute optimizations. Time range is unix nanos

func (e *MetricsEvaluator) Do(ctx context.Context, f SpansetFetcher, fetcherStart, fetcherEnd uint64, maxSeries int) error {
	// Make a copy of the request so we can modify it.
	storageReq := *e.storageReq

	if fetcherStart > 0 && fetcherEnd > 0 &&
		// exclude special case for a block with the same start and end
		fetcherStart != fetcherEnd {
		// Dynamically decide whether to use the trace-level timestamp columns
		// for filtering.
		overlap := timeRangeOverlap(e.start, e.end, fetcherStart, fetcherEnd)

		if overlap == 0.0 {
			// This shouldn't happen but might as well check.
			// No overlap == nothing to do
			return nil
		}

		// Our heuristic is if the overlap between the given fetcher (i.e. block)
		// and the request is less than X%, use them.  Above X%, the cost of loading
		// them doesn't outweight the benefits. The default 20% was measured in
		// local benchmarking.
		if overlap < e.timeOverlapCutoff {
			storageReq.StartTimeUnixNanos = e.start
			storageReq.EndTimeUnixNanos = e.end // Should this be exclusive?
		}
	}

	fetch, err := f.Fetch(ctx, storageReq)
	if errors.Is(err, util.ErrUnsupported) {
		return nil
	}
	if err != nil {
		return err
	}

	defer fetch.Results.Close()

	seriesCount := 0

	for {
		ss, err := fetch.Results.Next(ctx)
		if err != nil {
			return err
		}
		if ss == nil {
			break
		}

		e.mtx.Lock()

		if e.storageReq.TraceSampler != nil {
			e.storageReq.TraceSampler.Measured()
		}

		var validSpansCount int
		var randomSpanIndex int

		needExemplar := e.maxExemplars > 0 && e.sampleExemplar(ss.TraceID)

		if len(e.fragments) > 0 {
			// Batch mode: run each fragment's eval to filter spans, then observe matches.
			// Save original spans since Pipeline.evaluate modifies the spanset in-place.
			originalSpans := ss.Spans

			for j := range e.fragments {
				// Copy the span slice so eval can safely modify it
				ss.Spans = make([]Span, len(originalSpans))
				copy(ss.Spans, originalSpans)

				filtered, err := e.fragments[j].eval([]*Spanset{ss})
				if err != nil {
					continue
				}

				for _, fss := range filtered {
					for _, s := range fss.Spans {
						if e.checkTime {
							st := s.StartTimeUnixNanos()
							if st <= e.start || st > e.end {
								continue
							}
						}

						validSpansCount++
						e.fragments[j].pipeline.observe(s)
					}
				}
			}

			// Restore original spans for release
			ss.Spans = originalSpans

			// Sum series count across all fragments
			seriesCount = 0
			for j := range e.fragments {
				seriesCount += e.fragments[j].pipeline.length()
			}
		} else {
			// Single query mode (backward compatible)
			for i, s := range ss.Spans {

				if e.checkTime {
					st := s.StartTimeUnixNanos()
					if st <= e.start || st > e.end {
						continue
					}
				}

				if e.storageReq.SpanSampler != nil {
					e.storageReq.SpanSampler.Measured()
				}

				validSpansCount++
				e.metricsPipeline.observe(s)

				if !needExemplar {
					continue
				}

				// Reservoir sampling - select a random span for exemplar
				// Each span has a 1/validSpansCount probability of being selected
				if validSpansCount == 1 || rand.Intn(validSpansCount) == 0 {
					randomSpanIndex = i
				}
			}

			if needExemplar && validSpansCount > 0 {
				e.metricsPipeline.observeExemplar(ss.Spans[randomSpanIndex])
			}

			seriesCount = e.metricsPipeline.length()
		}

		e.spansTotal += uint64(validSpansCount)

		e.mtx.Unlock()
		ss.Release()

		if maxSeries > 0 && seriesCount >= maxSeries {
			break
		}
	}

	e.mtx.Lock()
	defer e.mtx.Unlock()
	e.bytes += fetch.Bytes()

	return nil
}

func (e *MetricsEvaluator) Length() int {
	if len(e.fragments) > 0 {
		total := 0
		for i := range e.fragments {
			total += e.fragments[i].pipeline.length()
		}
		return total
	}
	return e.metricsPipeline.length()
}

func (e *MetricsEvaluator) Metrics() (uint64, uint64, uint64) {
	e.mtx.Lock()
	defer e.mtx.Unlock()

	return e.bytes, e.spansTotal, e.spansDeduped
}

func (e *MetricsEvaluator) Results() SeriesSet {
	e.mtx.Lock()
	defer e.mtx.Unlock()

	spanMultiplier := 1.0
	if e.storageReq.SpanSampler != nil {
		spanMultiplier = e.storageReq.SpanSampler.FinalScalingFactor()
	}
	traceMultiplier := 1.0
	if e.storageReq.TraceSampler != nil {
		traceMultiplier = e.storageReq.TraceSampler.FinalScalingFactor()
	}

	multiplier := spanMultiplier * traceMultiplier

	if len(e.fragments) > 0 {
		// Batch mode: combine results from all fragments with _meta_query_fragment labels
		combined := make(SeriesSet)
		for _, frag := range e.fragments {
			fragLabel := Label{
				Name:  internalLabelQueryFragment,
				Value: NewStaticString(frag.ID),
			}
			for _, series := range frag.pipeline.result(multiplier) {
				// Add fragment label to each series
				series.Labels = series.Labels.Add(fragLabel)
				// Update key to include fragment label
				newKey := series.Labels.MapKey()
				combined[newKey] = series
			}
		}
		return combined
	}

	// NOTE: skip processing of second stage because not all first stage functions can't be pushed down.
	// for example: if query has avg_over_time(), then we can't push it down to second stage, and second stage
	// can only be processed on the frontend.
	// we could do this but it would require knowing if the first stage functions
	// can be pushed down to second stage or not so we are skipping it for now, and will handle it later.
	ss := e.metricsPipeline.result(multiplier)

	return ss
}

func (e *MetricsEvaluator) sampleExemplar(id []byte) bool {
	if len(e.exemplarMap) >= e.maxExemplars {
		return false
	}
	if len(id) == 0 {
		return false
	}

	// Avoid sampling exemplars for the same trace
	// Check does zero allocs
	if _, ok := e.exemplarMap[string(id)]; ok {
		return false
	}

	e.exemplarMap[string(id)] = struct{}{}
	e.exemplarCount++
	return true
}

// MetricsFrontendEvaluator pipes the sharded job results back into the engine for the rest
// of the pipeline.  i.e. This evaluator is for the query-frontend.
type MetricsFrontendEvaluator struct {
	mtx                sync.Mutex
	metricsPipeline    firstStageElement
	metricsSecondStage secondStageElement

	// MetricsMath L2/L3 support.
	// fragmentPipelines maps fragment ID -> pipeline for routing incoming series.
	// When non-empty, ObserveSeries routes by _meta_query_fragment label.
	fragmentPipelines map[string]firstStageElement
	// metricsMath is the math tree, set only in Final mode.
	// When set, Results() applies the math on fragment results.
	metricsMath *MetricsMathOp
}

func (m *MetricsFrontendEvaluator) ObserveSeries(in []*tempopb.TimeSeries) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	if len(m.fragmentPipelines) > 0 {
		m.observeSeriesFragments(in)
		return
	}

	m.metricsPipeline.observeSeries(in)
}

// observeSeriesFragments routes incoming series to the correct fragment pipeline
// based on the _meta_query_fragment label. The label is stripped before feeding
// to the pipeline so it doesn't pollute internal MapKeys.
func (m *MetricsFrontendEvaluator) observeSeriesFragments(in []*tempopb.TimeSeries) {
	// Batch series per fragment for efficiency
	grouped := make(map[string][]*tempopb.TimeSeries, len(m.fragmentPipelines))

	for _, ts := range in {
		fragmentID, strippedTS := stripFragmentLabel(ts)
		if fragmentID == "" {
			continue // no _meta_query_fragment label
		}
		if _, ok := m.fragmentPipelines[fragmentID]; !ok {
			continue // unknown fragment
		}
		grouped[fragmentID] = append(grouped[fragmentID], strippedTS)
	}

	for id, batch := range grouped {
		m.fragmentPipelines[id].observeSeries(batch)
	}
}

func (m *MetricsFrontendEvaluator) Results() SeriesSet {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	if len(m.fragmentPipelines) > 0 {
		return m.metricsMathResults()
	}

	// Job results are not scaled by sampling, but this is here for the interface.
	results := m.metricsPipeline.result(1.0)

	if m.metricsSecondStage != nil {
		// metrics second stage is only set when query has second stage function and mode = final
		// if we have metrics second stage, pass first stage results through
		// second stage for further processing.
		results = m.metricsSecondStage.process(results)
	}

	return results
}

// metricsMathResults returns results for MetricsMath queries.
// In Final mode (metricsMath != nil): applies the math tree and returns combined series.
// In Sum mode (metricsMath == nil): returns fragment-labeled series.
func (m *MetricsFrontendEvaluator) metricsMathResults() SeriesSet {
	// Collect results from each fragment pipeline
	fragmentResults := make(map[string]SeriesSet, len(m.fragmentPipelines))
	for id, pipeline := range m.fragmentPipelines {
		fragmentResults[id] = pipeline.result(1.0)
	}

	if m.metricsMath != nil {
		// Final mode: apply math tree to produce combined series
		return evaluateMetricsMathOp(m.metricsMath, fragmentResults)
	}

	// Sum mode: return fragment-labeled series
	combined := make(SeriesSet)
	for id, ss := range fragmentResults {
		fragLabel := Label{
			Name:  internalLabelQueryFragment,
			Value: NewStaticString(id),
		}
		for _, series := range ss {
			series.Labels = series.Labels.Add(fragLabel)
			newKey := series.Labels.MapKey()
			combined[newKey] = series
		}
	}
	return combined
}

func (m *MetricsFrontendEvaluator) Length() int {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	if len(m.fragmentPipelines) > 0 {
		total := 0
		for _, p := range m.fragmentPipelines {
			total += p.length()
		}
		return total
	}

	return m.metricsPipeline.length()
}

type SeriesAggregator interface {
	Combine([]*tempopb.TimeSeries)
	Results() SeriesSet
	Length() int
}

type SimpleAggregationOp int

const (
	sumAggregation SimpleAggregationOp = iota
	minOverTimeAggregation
	maxOverTimeAggregation
	sumOverTimeAggregation
)

type SimpleAggregator struct {
	ss              SeriesSet
	exemplarBuckets bucketSet
	intervalMapper  IntervalMapper
	aggregationFunc func(existingValue float64, newValue float64) float64
	initWithNaN     bool
}

func NewSimpleCombiner(req *tempopb.QueryRangeRequest, op SimpleAggregationOp, exemplars uint32) *SimpleAggregator {
	var initWithNaN bool
	var f func(existingValue float64, newValue float64) float64
	switch op {
	case minOverTimeAggregation:
		// Simple min aggregator. It calculates the minimum between existing values and a new sample
		f = minOverTime()
		initWithNaN = true
	case maxOverTimeAggregation:
		// Simple max aggregator. It calculates the maximum between existing values and a new sample
		f = maxOverTime()
		initWithNaN = true
	case sumOverTimeAggregation:
		f = sumOverTime()
		initWithNaN = true
	default:
		// Simple addition aggregator. It adds existing values with the new sample.
		f = func(existingValue float64, newValue float64) float64 { return existingValue + newValue }
		initWithNaN = false

	}
	return &SimpleAggregator{
		ss:              make(SeriesSet),
		exemplarBuckets: newExemplarBucketSet(exemplars, req.Start, req.End, req.Step, IsInstant(req)),
		intervalMapper:  NewIntervalMapperFromReq(req),
		aggregationFunc: f,
		initWithNaN:     initWithNaN,
	}
}

func (b *SimpleAggregator) Combine(in []*tempopb.TimeSeries) {
	nan := math.Float64frombits(normalNaN)

	for _, ts := range in {
		// Convert proto labels to traceql labels
		labels, _ := convertProtoLabelsToTraceQL(ts.Labels, false)
		key := labels.MapKey()

		existing, ok := b.ss[key]
		if !ok {

			existing = TimeSeries{
				Labels:    labels,
				Values:    make([]float64, b.intervalMapper.IntervalCount()),
				Exemplars: make([]Exemplar, 0, len(ts.Exemplars)),
			}
			if b.initWithNaN {
				for i := range existing.Values {
					existing.Values[i] = nan
				}
			}

			b.ss[key] = existing
		}

		for _, sample := range ts.Samples {
			j := b.intervalMapper.IntervalMs(sample.TimestampMs)
			if j >= 0 && j < len(existing.Values) {
				existing.Values[j] = b.aggregationFunc(existing.Values[j], sample.Value)
			}
		}

		b.aggregateExemplars(ts, &existing)

		b.ss[key] = existing
	}
}

func (b *SimpleAggregator) aggregateExemplars(ts *tempopb.TimeSeries, existing *TimeSeries) {
	for _, exemplar := range ts.Exemplars {
		if b.exemplarBuckets.testTotal() {
			break
		}
		if b.exemplarBuckets.addAndTest(uint64(exemplar.TimestampMs)) { //nolint: gosec // G115
			continue // Skip this exemplar and continue, next exemplar might fit in a different bucket
		}
		labels, _ := convertProtoLabelsToTraceQL(exemplar.Labels, false)
		existing.Exemplars = append(existing.Exemplars, Exemplar{
			Labels:      labels,
			Value:       exemplar.Value,
			TimestampMs: uint64(exemplar.TimestampMs),
		})
	}
}

func (b *SimpleAggregator) Results() SeriesSet {
	return b.ss
}

func (b *SimpleAggregator) Length() int {
	return len(b.ss)
}

type HistogramBucket struct {
	Max   float64
	Count int
}

type Histogram struct {
	Buckets []HistogramBucket
}

func (h *Histogram) Record(bucket float64, count int) {
	for i := range h.Buckets {
		if h.Buckets[i].Max == bucket {
			h.Buckets[i].Count += count
			return
		}
	}

	h.Buckets = append(h.Buckets, HistogramBucket{
		Max:   bucket,
		Count: count,
	})
}

type IntervalMapper interface {
	// Interval the given timestamp within the range and step.
	Interval(ts uint64) int
	// IntervalMs is the same as Bucket except the input and calculations are in unix milliseconds.
	IntervalMs(ts int64) int
	TimestampOf(interval int) uint64
	// IntervalCount is the number of intervals in the range with step.
	IntervalCount() int
}

func NewIntervalMapperFromReq(req *tempopb.QueryRangeRequest) IntervalMapper {
	return NewIntervalMapper(req.Start, req.End, req.Step, IsInstant(req))
}

func NewIntervalMapper(start, end, step uint64, instant bool) IntervalMapper {
	startMs := start - start%uint64(time.Millisecond)
	endMs := end - end%uint64(time.Millisecond)
	if instant {
		return &IntervalMapperInstant{
			start:   start,
			end:     end,
			startMs: startMs,
			endMs:   endMs,
		}
	}
	mapper := &IntervalMapperQueryRange{
		start:   alignStart(start, end, step, instant),
		end:     alignEnd(start, end, step, instant),
		step:    step,
		startMs: alignStart(startMs, endMs, step, instant),
		endMs:   alignEnd(startMs, endMs, step, instant),
	}
	// pre-calculate intervals
	intervals := (mapper.end - mapper.start) / mapper.step
	mapper.intervalCount = int(intervals)
	return mapper
}

type IntervalMapperQueryRange struct {
	start, end, step uint64
	startMs, endMs   uint64
	intervalCount    int
}

func (i IntervalMapperQueryRange) Interval(ts uint64) int {
	return i.interval(ts, i.start, i.end, i.step)
}

func (i IntervalMapperQueryRange) IntervalMs(tsmill int64) int {
	ts := uint64(time.Duration(tsmill) * time.Millisecond)
	// TODO: step is not truncated to milliseconds, it might be a bug
	return i.interval(ts, i.startMs, i.endMs, i.step)
}

func (i IntervalMapperQueryRange) interval(ts, start, end, step uint64) int {
	if !i.isTsValid(ts, start, end, step) {
		return -1
	}
	if ts <= start { // to avoid overflow
		return 0 // if pass validation and less than start, always first interval
	}

	offset := ts - start
	// Calculate which interval the timestamp falls into
	// Since intervals are right-closed: (start; start+step], (start+step; start+2*step], etc.
	// we need to handle the case where ts is exactly on a step boundary
	interval := offset / step
	if interval*step == offset { // the same as offset % step == 0
		// ts is exactly on a step boundary, so it belongs to the previous interval
		interval--
	}
	return int(interval)
}

func (i IntervalMapperQueryRange) TimestampOf(interval int) uint64 {
	// start as initial offset plus interval's offset
	return i.start + (uint64(interval)+1)*i.step
}

func (i IntervalMapperQueryRange) IntervalCount() int {
	return i.intervalCount
}

func (i IntervalMapperQueryRange) isTsValid(ts, start, end, step uint64) bool {
	return ts > start && ts <= end && end != start && step != 0
}

type IntervalMapperInstant struct {
	start, end     uint64
	startMs, endMs uint64
}

func (i IntervalMapperInstant) Interval(ts uint64) int {
	if !i.isTsValid(ts, i.start, i.end) {
		return -1
	}
	return 0 // Instant queries only have one bucket
}

func (i IntervalMapperInstant) IntervalMs(tsmill int64) int {
	ts := uint64(time.Duration(tsmill) * time.Millisecond)
	if !i.isTsValid(ts, i.startMs, i.endMs) {
		return -1
	}

	return 0 // Instant queries only have one bucket
}

func (i IntervalMapperInstant) IntervalCount() int {
	return 1 // Instant queries only have one bucket
}

func (i IntervalMapperInstant) TimestampOf(int) uint64 {
	return i.end
}

func (i IntervalMapperInstant) isTsValid(ts, start, end uint64) bool {
	return ts >= start && ts <= end && end != start
}

type histSeries struct {
	labels          Labels
	hist            []Histogram
	exemplars       []Exemplar
	exemplarBuckets bucketSet
}

type HistogramAggregator struct {
	ss             map[string]histSeries
	qs             []float64
	intervalMapper IntervalMapper
	exemplarLimit  uint32
	// Reusable buffer for all label processing
	labelBuffer            Labels
	exemplarBucketsCreator func() bucketSet
}

func NewHistogramAggregator(req *tempopb.QueryRangeRequest, qs []float64, exemplarLimit uint32) *HistogramAggregator {
	return &HistogramAggregator{
		ss:             make(map[string]histSeries),
		qs:             qs,
		intervalMapper: NewIntervalMapperFromReq(req),
		exemplarLimit:  exemplarLimit,
		labelBuffer:    make(Labels, 0, 8), // Pre-allocate with reasonable capacity
		exemplarBucketsCreator: func() bucketSet {
			return newExemplarBucketSet(exemplarLimit, req.Start, req.End, req.Step, IsInstant(req))
		},
	}
}

func (h *HistogramAggregator) Combine(in []*tempopb.TimeSeries) {
	for _, ts := range in {
		// Convert proto labels to traceql labels while stripping the bucket label
		var bucket Static
		h.labelBuffer, bucket = convertProtoLabelsToTraceQL(ts.Labels, true)

		if bucket.Type == TypeNil {
			// Bad __bucket label?
			continue
		}

		withoutBucketStr := h.labelBuffer.String()

		existing, ok := h.ss[withoutBucketStr]
		if !ok {
			// Create a copy of the labels since the buffer will be reused
			withoutBucket := make(Labels, len(h.labelBuffer))
			copy(withoutBucket, h.labelBuffer)

			// Create a fresh histogram slice for each series to avoid sharing memory
			// Pool reuse was causing data contamination between series
			histSlice := make([]Histogram, h.intervalMapper.IntervalCount())

			existing = histSeries{
				labels: withoutBucket,
				hist:   histSlice,
				// Pre-allocate exemplars slice with capacity hint to reduce allocations
				exemplars:       make([]Exemplar, 0, len(ts.Exemplars)),
				exemplarBuckets: h.exemplarBucketsCreator(),
			}
		}

		b := bucket.Float()

		for _, sample := range ts.Samples {
			if sample.Value == 0 {
				continue
			}
			j := h.intervalMapper.IntervalMs(sample.TimestampMs)
			if j >= 0 && j < len(existing.hist) {
				existing.hist[j].Record(b, int(sample.Value))
			}
		}

		// Collect exemplars per series, not globally
		for _, exemplar := range ts.Exemplars {
			if existing.exemplarBuckets.testTotal() {
				break
			}
			if existing.exemplarBuckets.addAndTest(uint64(exemplar.TimestampMs)) {
				continue // Skip this exemplar and continue, next exemplar might fit in a different bucket
			}

			// Convert exemplar labels directly (no need for buffer reuse here)
			labels, _ := convertProtoLabelsToTraceQL(exemplar.Labels, false)

			existing.exemplars = append(existing.exemplars, Exemplar{
				Labels:      labels,
				Value:       exemplar.Value,
				TimestampMs: uint64(exemplar.TimestampMs),
			})
		}
		h.ss[withoutBucketStr] = existing
	}
}

func (h *HistogramAggregator) Results() SeriesSet {
	results := make(SeriesSet, len(h.ss)*len(h.qs))

	// Aggregate buckets across all series and time intervals for better quantile calculation
	aggregatedBuckets := make(map[float64]int) // bucketMax -> totalCount

	for _, in := range h.ss {
		// Aggregate bucket counts across all time intervals
		for _, hist := range in.hist {
			for _, bucket := range hist.Buckets {
				aggregatedBuckets[bucket.Max] += bucket.Count
			}
		}
	}

	// Calculate quantile values from aggregated distribution
	// Convert map to sorted slice
	buckets := make([]HistogramBucket, 0, len(aggregatedBuckets))
	for bucketMax, count := range aggregatedBuckets {
		buckets = append(buckets, HistogramBucket{
			Max:   bucketMax,
			Count: count,
		})
	}

	sort.Slice(buckets, func(i, j int) bool {
		return buckets[i].Max < buckets[j].Max
	})

	quantileValues := make([]float64, len(h.qs))
	for i, q := range h.qs {
		quantileValues[i] = Log2Quantile(q, buckets)
	}

	// Build results using the calculated quantile values
	for _, in := range h.ss {
		for i := range in.hist {
			if len(in.hist[i].Buckets) > 0 {
				// Sort interval buckets in place for performance
				sort.Slice(in.hist[i].Buckets, func(a, b int) bool {
					return in.hist[i].Buckets[a].Max < in.hist[i].Buckets[b].Max
				})
			}
		}

		// For each input series, we create a new series for each quantile.
		for qIdx, q := range h.qs {
			// Append label for the quantile
			labels := append((Labels)(nil), in.labels...)
			labels = append(labels, Label{"p", NewStaticFloat(q)})

			ts := TimeSeries{
				Labels: labels,
				Values: make([]float64, len(in.hist)),
			}

			for i := range in.hist {
				if len(in.hist[i].Buckets) == 0 {
					ts.Values[i] = 0.0
					continue
				}

				// Use sorted buckets for quantile calculation
				ts.Values[i] = Log2Quantile(q, in.hist[i].Buckets)
			}

			// Select exemplars for this quantile using simplified assignment logic
			for _, exemplar := range in.exemplars {
				if h.assignExemplarToQuantile(exemplar.Value, quantileValues, buckets) == qIdx {
					ts.Exemplars = append(ts.Exemplars, exemplar)
				}
			}

			results[labels.MapKey()] = ts
		}
	}

	return results
}

// assignExemplarToQuantile determines which quantile (if any) an exemplar should be assigned to.
// Returns the quantile index, or -1 if the exemplar doesn't fit any quantile reasonably well.
// This uses a simple closest-match strategy with reasonable bucket validation.
func (h *HistogramAggregator) assignExemplarToQuantile(exemplarValue float64, quantileValues []float64, buckets []HistogramBucket) int {
	if len(quantileValues) == 0 || len(buckets) == 0 {
		return -1
	}

	bestIdx := -1
	bestDiff := math.Inf(1)

	for i, quantileValue := range quantileValues {
		// Find the closest quantile value for better visual alignment
		diff := math.Abs(exemplarValue - quantileValue)
		if diff < bestDiff {
			bestDiff = diff
			bestIdx = i
		}
	}

	return bestIdx
}

func (h *HistogramAggregator) Length() int {
	return len(h.ss) * len(h.qs)
}

// Log2Bucketize rounds the given value to the next powers-of-two bucket.
func Log2Bucketize(v uint64) float64 {
	if v < 2 {
		return -1
	}

	return float64(uint64(1) << (64 - bits.LeadingZeros64(v-1)))

	// OLD:
	// return math.Pow(2, math.Ceil(math.Log2(float64(v))))
}

// Log2Quantile returns the quantile given bucket labeled with float ranges and counts. Uses
// exponential power-of-two interpolation between buckets as needed.
func Log2Quantile(p float64, buckets []HistogramBucket) float64 {
	value, _ := Log2QuantileWithBucket(p, buckets)
	return value
}

// Log2QuantileWithBucket returns both the quantile value and the bucket index where the quantile falls.
// This avoids rescanning buckets when you need to determine which bucket contains a specific value.
func Log2QuantileWithBucket(p float64, buckets []HistogramBucket) (float64, int) {
	if math.IsNaN(p) ||
		p < 0 ||
		p > 1 ||
		len(buckets) == 0 {
		return 0, -1
	}

	totalCount := 0
	for _, b := range buckets {
		totalCount += b.Count
	}

	if totalCount == 0 {
		return 0, -1
	}

	// Maximum amount of samples to include. We round up to better handle
	// percentiles on low sample counts (<100).
	maxSamples := int(math.Ceil(p * float64(totalCount)))

	if maxSamples == 0 {
		// We have to read at least one sample.
		maxSamples = 1
	}

	// Find the bucket where the percentile falls in.
	var total, bucket int
	for i, b := range buckets {
		// Next bucket
		bucket = i

		// If we can't fully consume the samples in this bucket
		// then we are done.
		if total+b.Count > maxSamples {
			break
		}

		// Consume all samples in this bucket
		total += b.Count

		// p100 or happen to read the exact number of samples.
		// Quantile is the max range for the bucket. No reason
		// to enter interpolation below.
		if total == maxSamples {
			return b.Max, bucket
		}
	}

	// Fraction to interpolate between buckets, sample-count wise.
	// 0.5 means halfway
	interp := float64(maxSamples-total) / float64(buckets[bucket].Count)

	// Exponential interpolation between buckets
	// The current bucket represents the maximum value
	maxV := math.Log2(buckets[bucket].Max)
	var minV float64
	if bucket > 0 {
		// Prior bucket represents the min
		minV = math.Log2(buckets[bucket-1].Max)
	} else {
		// There is no prior bucket, assume powers of 2
		minV = maxV - 1
	}
	return math.Pow(2, minV+(maxV-minV)*interp), bucket
}

var (
	_ SeriesAggregator = (*SimpleAggregator)(nil)
	_ SeriesAggregator = (*HistogramAggregator)(nil)
)

func FloatizeAttribute(s Span, a Attribute) (float64, StaticType) {
	v, ok := s.AttributeFor(a)
	if !ok {
		return 0, TypeNil
	}

	f := v.Float()
	if math.IsNaN(f) {
		return 0, TypeNil
	}
	return f, v.Type
}

// convertProtoLabelsToTraceQL converts protobuf labels to traceql Labels format.
// If skipBucket is true, it will skip any label with key matching internalLabelBucket.
// Returns the converted labels and the bucket value (if found and not skipped).
func convertProtoLabelsToTraceQL(protoLabels []commonv1proto.KeyValue, skipBucket bool) (Labels, Static) {
	// TODO: consider memory pooling for Labels and Static to reduce allocations
	labels := make(Labels, 0, len(protoLabels))
	var bucket Static

	for i := range protoLabels {
		l := &protoLabels[i]
		if skipBucket && l.Key == internalLabelBucket {
			bucket = StaticFromAnyValue(l.Value)
			continue
		}
		labels = append(labels, Label{
			Name:  l.Key,
			Value: StaticFromAnyValue(l.Value),
		})
	}

	return labels, bucket
}

// processTopK implements TopKBottomK topk method
func processTopK(input SeriesSet, valueLength, limit int) SeriesSet {
	result := make(SeriesSet)
	// Min heap for top-k (smallest values at top for easy replacement)
	h := &seriesHeap{}
	heap.Init(h)

	// process each timestamp
	for i := 0; i < valueLength; i++ {
		// process each series for this timestamp
		for key, series := range input {
			if i >= len(series.Values) {
				continue
			}

			value := series.Values[i]
			if math.IsNaN(value) {
				continue // Skip NaN values
			}

			// If heap not full yet, add the value
			if h.Len() < limit {
				heap.Push(h, seriesValue{
					key:   key,
					value: value,
				})
				continue
			}

			// If new value is greater than smallest in heap, replace it
			// For ties (equal values), use series key comparison to maintain determinism
			smallest := (*h)[0]
			if dataPointGreaterThan(seriesValue{key: key, value: value}, smallest) {
				heap.Pop(h)
				heap.Push(h, seriesValue{
					key:   key,
					value: value,
				})
			}
		}

		// we have iterated over all series for this timestamp
		// empty the heap and record these series in result set
		for h.Len() > 0 {
			sv := heap.Pop(h).(seriesValue)
			initSeriesInResult(result, sv.key, input, valueLength)
			// Set only this timestamp's value
			result[sv.key].Values[i] = input[sv.key].Values[i]
		}
	}

	return result
}

// processBottomK implements TopKBottomK bottomk method
func processBottomK(input SeriesSet, valueLength, limit int) SeriesSet {
	result := make(SeriesSet)

	// Max heap for bottom-k (largest values at top for easy replacement)
	h := &reverseSeriesHeap{}
	heap.Init(h)

	// Process each timestamp
	for i := 0; i < valueLength; i++ {
		// Process each series for this timestamp
		for key, series := range input {
			if i >= len(series.Values) {
				continue
			}

			value := series.Values[i]
			if math.IsNaN(value) {
				continue // Skip NaN values
			}

			// If heap not full yet, add the value
			if h.Len() < limit {
				heap.Push(h, seriesValue{
					key:   key,
					value: value,
				})
				continue
			}

			// If new value is less than largest in heap, replace it
			// For ties (equal values), use series key comparison to maintain determinism
			largest := (*h)[0]
			if dataPointLessThan(seriesValue{key: key, value: value}, largest) {
				heap.Pop(h)
				heap.Push(h, seriesValue{
					key:   key,
					value: value,
				})
			}
		}

		// we have iterated over all series for this timestamp
		// empty the heap and record these series in result set
		for h.Len() > 0 {
			sv := heap.Pop(h).(seriesValue)
			initSeriesInResult(result, sv.key, input, valueLength)
			// Set only this timestamp's value
			result[sv.key].Values[i] = input[sv.key].Values[i]
		}
	}

	return result
}

// initSeriesInResult ensures that a series exists in the result map, and
// initializes it with NaN values if it doesn't exist in the result set.
func initSeriesInResult(result SeriesSet, key SeriesMapKey, input SeriesSet, valueLength int) {
	if _, exists := result[key]; exists {
		// series already exists, no need to initialize
		return
	}
	// series doesn't exist, initialize it
	// Copy the series labels and exemplars from the input
	result[key] = TimeSeries{
		Labels:    input[key].Labels,
		Values:    make([]float64, valueLength),
		Exemplars: input[key].Exemplars,
	}

	// Initialize all values to NaN because we only want to set values for this timestamp
	for j := range result[key].Values {
		result[key].Values[j] = math.NaN()
	}
}

package tempodb

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/grafana/tempo/v3/pkg/tempopb"
	v1 "github.com/grafana/tempo/v3/pkg/tempopb/common/v1"
	v1_trace "github.com/grafana/tempo/v3/pkg/tempopb/trace/v1"
	"github.com/grafana/tempo/v3/pkg/util/test"
	"github.com/grafana/tempo/v3/tempodb/backend"
	"github.com/grafana/tempo/v3/tempodb/encoding"
	"github.com/grafana/tempo/v3/tempodb/encoding/common"
	"github.com/grafana/tempo/v3/tempodb/encoding/vparquet3"
	"github.com/grafana/tempo/v3/tempodb/encoding/vparquet4"
	"github.com/grafana/tempo/v3/tempodb/encoding/vparquet5"
	"github.com/stretchr/testify/require"
)

const secretPrefix = "enc:v1:abcbdbc:"

func attrString(key, value string) *v1.KeyValue {
	return &v1.KeyValue{Key: key, Value: &v1.AnyValue{Value: &v1.AnyValue_StringValue{StringValue: value}}}
}

func attrStrings(key string, values ...string) *v1.KeyValue {
	items := make([]*v1.AnyValue, 0, len(values))
	for _, value := range values {
		items = append(items, &v1.AnyValue{Value: &v1.AnyValue_StringValue{StringValue: value}})
	}
	return &v1.KeyValue{Key: key, Value: &v1.AnyValue{Value: &v1.AnyValue_ArrayValue{
		ArrayValue: &v1.ArrayValue{Values: items},
	}}}
}

func hasAttribute(attrs []*v1.KeyValue, key string) bool {
	for _, attr := range attrs {
		if attr.Key == key {
			return true
		}
	}
	return false
}

func attributeValue(attrs []*v1.KeyValue, key string) string {
	for _, attr := range attrs {
		if attr.Key == key {
			return attr.GetValue().GetStringValue()
		}
	}
	return ""
}

type attributeTraceIterator struct {
	data []testData
	pos  int
}

func (i *attributeTraceIterator) Next(context.Context) (common.ID, *tempopb.Trace, error) {
	if i.pos == len(i.data) {
		return nil, nil, io.EOF
	}
	d := i.data[i.pos]
	i.pos++
	return d.id, d.t, nil
}

func (*attributeTraceIterator) Close() {}

func attributeTestBlock(t *testing.T, rw *readerWriter, version string, columns backend.DedicatedColumns, traces []testData) *backend.BlockMeta {
	t.Helper()
	enc, err := encoding.FromVersion(version)
	require.NoError(t, err)
	cfg := &common.BlockConfig{
		Version: version, RowGroupSizeBytes: 1_000_000,
		BloomFP: common.DefaultBloomFP, BloomShardSizeBytes: common.DefaultBloomShardSizeBytes,
		DedicatedColumns: columns,
	}
	now := time.Now()
	meta := &backend.BlockMeta{
		TenantID: testTenantID, BlockID: backend.NewUUID(),
		StartTime: now.Add(-time.Minute), EndTime: now.Add(time.Minute),
		TotalObjects: int64(len(traces)), DedicatedColumns: columns,
	}
	written, err := enc.CreateBlock(context.Background(), cfg, meta, &attributeTraceIterator{data: traces}, rw.r, rw.w)
	require.NoError(t, err)
	return written
}

func TestRedactBlockAttributesRetrieval(t *testing.T) {
	ctx := context.Background()
	for _, version := range []string{vparquet3.VersionString, vparquet4.VersionString, vparquet5.VersionString} {
		for _, scope := range []string{"span", "resource"} {
			for _, dedicated := range []bool{false, true} {
				t.Run(version+"/"+scope+"/dedicated="+map[bool]string{true: "yes", false: "no"}[dedicated], func(t *testing.T) {
					r, _, c, _ := testConfig(t, 0)
					rw := r.(*readerWriter)
					idMatch := make([]byte, 16)
					idMatch[15] = 1
					idKeep := make([]byte, 16)
					idKeep[15] = 2
					var columns backend.DedicatedColumns
					if dedicated {
						columns = backend.DedicatedColumns{
							{Scope: backend.DedicatedColumnScope(scope), Name: "retained", Type: backend.DedicatedColumnTypeString},
							{Scope: backend.DedicatedColumnScope(scope), Name: "enc.secret", Type: backend.DedicatedColumnTypeString},
						}
					}
					makeTrace := func(id []byte, secret string) *tempopb.Trace {
						batch := test.MakeBatchWithAttributes(0, id, []*v1.KeyValue{
							attrString("enc.secret", secret), attrStrings("bi.secret", "bi:v1:abcbdbc:first", "bi:v1:abcbdbc:second"), attrString("retained", "unchanged"),
						})
						start := uint64(time.Now().UnixNano())
						batch.ScopeSpans = []*v1_trace.ScopeSpans{{Spans: []*v1_trace.Span{
							test.MakeSpanWithTimeWindow(id, start, start+uint64(time.Second)),
							test.MakeSpanWithTimeWindow(id, start+uint64(time.Second), start+uint64(2*time.Second)),
						}}}
						for _, span := range batch.ScopeSpans[0].Spans {
							span.Attributes = append(span.Attributes, attrString("enc.secret", secret), attrStrings("bi.secret", "bi:v1:abcbdbc:first", "bi:v1:abcbdbc:second"), attrString("span.enc.secret", secret), attrString("retained", "unchanged"))
						}
						return &tempopb.Trace{ResourceSpans: []*v1_trace.ResourceSpans{batch}}
					}
					matching := makeTrace(idMatch, secretPrefix+"encrypted-payload")
					unmatched := makeTrace(idKeep, "plaintext")
					meta := attributeTestBlock(t, rw, version, columns, []testData{{id: idMatch, t: matching}, {id: idKeep, t: unmatched}})
					rule := &tempopb.AttributeRedaction{Key: scope + ".enc.secret", ValuePrefix: secretPrefix}

					for _, mode := range []tempopb.RedactionMode{tempopb.RedactionMode_REDACTION_MODE_DRY_RUN, tempopb.RedactionMode_REDACTION_MODE_APPLY} {
						rewrote, found, out, err := c.RedactBlockAttributes(ctx, meta, testTenantID, rule, mode, RedactionWindow{})
						require.NoError(t, err)
						require.Equal(t, 1, found, "count changed traces, not spans")
						if mode == tempopb.RedactionMode_REDACTION_MODE_DRY_RUN {
							require.False(t, rewrote)
							require.Nil(t, out)
							before, err := encoding.OpenBlock(meta, rw.r)
							require.NoError(t, err)
							result, err := before.FindTraceByID(ctx, idMatch, common.DefaultSearchOptions())
							require.NoError(t, err)
							require.Equal(t, secretPrefix+"encrypted-payload", attributeValue(result.Trace.ResourceSpans[0].Resource.Attributes, "enc.secret"))
							require.True(t, hasAttribute(result.Trace.ResourceSpans[0].Resource.Attributes, "bi.secret"), "dry-run must preserve the sidecar")
							continue
						}
						require.True(t, rewrote)
						require.NotNil(t, out)
						require.Equal(t, int64(2), out.TotalObjects)
						require.Equal(t, version, out.Version)
						require.ElementsMatch(t, columns, out.DedicatedColumns)
						block, err := encoding.OpenBlock(out, rw.r)
						require.NoError(t, err)
						result, err := block.FindTraceByID(ctx, idMatch, common.DefaultSearchOptions())
						require.NoError(t, err)
						require.NotNil(t, result.Trace)
						batch := result.Trace.ResourceSpans[0]
						require.Len(t, batch.ScopeSpans[0].Spans, 2)
						wantResource := secretPrefix + "encrypted-payload"
						wantSpan := secretPrefix + "encrypted-payload"
						if scope == "resource" {
							wantResource = common.RedactedAttributeValue
						} else {
							wantSpan = common.RedactedAttributeValue
						}
						require.Equal(t, wantResource, attributeValue(batch.Resource.Attributes, "enc.secret"))
						require.Equal(t, scope != "resource", hasAttribute(batch.Resource.Attributes, "bi.secret"))
						require.Equal(t, "unchanged", attributeValue(batch.Resource.Attributes, "retained"))
						for _, span := range batch.ScopeSpans[0].Spans {
							require.Equal(t, wantSpan, attributeValue(span.Attributes, "enc.secret"))
							require.Equal(t, secretPrefix+"encrypted-payload", attributeValue(span.Attributes, "span.enc.secret"), "do not treat scope prefix as part of OTLP key")
							require.Equal(t, scope != "span", hasAttribute(span.Attributes, "bi.secret"))
							require.Equal(t, "unchanged", attributeValue(span.Attributes, "retained"))
						}
						keep, err := block.FindTraceByID(ctx, idKeep, common.DefaultSearchOptions())
						require.NoError(t, err)
						require.NotNil(t, keep.Trace)
						require.Equal(t, "plaintext", attributeValue(keep.Trace.ResourceSpans[0].Resource.Attributes, "enc.secret"))
						require.True(t, hasAttribute(keep.Trace.ResourceSpans[0].Resource.Attributes, "bi.secret"), "unmatched sidecar must survive")
						noRewrite, noFound, noMeta, err := c.RedactBlockAttributes(ctx, out, testTenantID, rule, tempopb.RedactionMode_REDACTION_MODE_APPLY, RedactionWindow{})
						require.NoError(t, err)
						// In the opposite scope there is still an unredacted copy; a second
						// rewrite is appropriate only if the scoped value itself remains.
						require.False(t, noRewrite)
						require.Zero(t, noFound)
						require.Nil(t, noMeta)
					}
				})
			}
		}
	}
}

func TestRedactBlockAttributePairsAtomicAndScoped(t *testing.T) {
	ctx := context.Background()
	kid := strings.Repeat("a", 32)
	otherKid := strings.Repeat("b", 32)
	rules := []*tempopb.AttributeRedaction{
		{Key: "resource.enc.alpha", ValuePrefix: "enc:v1:" + kid},
		{Key: "resource.bi.alpha", ValuePrefix: "bi:v1:" + kid},
		{Key: "span.enc.beta", ValuePrefix: "enc:v1:" + otherKid},
		{Key: "span.bi.beta", ValuePrefix: "bi:v1:" + otherKid},
	}
	for _, version := range []string{vparquet3.VersionString, vparquet4.VersionString, vparquet5.VersionString} {
		t.Run(version, func(t *testing.T) {
			r, _, c, _ := testConfig(t, 0)
			rw := r.(*readerWriter)
			makeTrace := func(id []byte, matched bool) *tempopb.Trace {
				encAlpha, encBeta := "enc:v1:"+kid+"0:wrong-key", "enc:v1:"+otherKid+"0:wrong-key"
				if matched {
					encAlpha, encBeta = "enc:v1:"+kid+":encrypted", "enc:v1:"+otherKid+":encrypted"
				}
				batch := test.MakeBatchWithAttributes(0, id, []*v1.KeyValue{
					attrString("enc.alpha", encAlpha),
					attrStrings("bi.alpha", "bi:v1:"+kid+":first", "bi:v1:"+kid+":second"),
					attrString("enc.beta", "enc:v1:"+otherKid+":resource-untouched"),
					attrStrings("bi.beta", "bi:v1:"+otherKid+":resource-untouched"),
				})
				now := uint64(time.Now().UnixNano())
				span := test.MakeSpanWithTimeWindow(id, now, now+uint64(time.Second))
				span.Attributes = append(span.Attributes,
					attrString("enc.beta", encBeta),
					attrStrings("bi.beta", "bi:v1:"+otherKid+":first", "bi:v1:"+otherKid+":second"),
					attrString("enc.alpha", "enc:v1:"+kid+":span-untouched"),
					attrStrings("bi.alpha", "bi:v1:"+kid+":span-untouched"),
				)
				batch.ScopeSpans = []*v1_trace.ScopeSpans{{Spans: []*v1_trace.Span{span}}}
				return &tempopb.Trace{ResourceSpans: []*v1_trace.ResourceSpans{batch}}
			}
			idMatch, idKeep := make([]byte, 16), make([]byte, 16)
			idMatch[15], idKeep[15] = 1, 2
			columns := backend.DedicatedColumns{
				{Scope: backend.DedicatedColumnScopeResource, Name: "enc.alpha", Type: backend.DedicatedColumnTypeString},
				{Scope: backend.DedicatedColumnScopeSpan, Name: "enc.beta", Type: backend.DedicatedColumnTypeString},
			}
			meta := attributeTestBlock(t, rw, version, columns, []testData{{id: idMatch, t: makeTrace(idMatch, true)}, {id: idKeep, t: makeTrace(idKeep, false)}})
			rewrote, found, replacement, err := c.RedactBlockAttributePairs(ctx, meta, testTenantID, rules, tempopb.RedactionMode_REDACTION_MODE_DRY_RUN, RedactionWindow{})
			require.NoError(t, err)
			require.False(t, rewrote)
			require.Equal(t, 1, found)
			require.Nil(t, replacement)

			before, err := encoding.OpenBlock(meta, rw.r)
			require.NoError(t, err)
			original, err := before.FindTraceByID(ctx, idMatch, common.DefaultSearchOptions())
			require.NoError(t, err)
			require.Equal(t, "enc:v1:"+kid+":encrypted", attributeValue(original.Trace.ResourceSpans[0].Resource.Attributes, "enc.alpha"))
			require.True(t, hasAttribute(original.Trace.ResourceSpans[0].Resource.Attributes, "bi.alpha"))

			rewrote, found, replacement, err = c.RedactBlockAttributePairs(ctx, meta, testTenantID, rules, tempopb.RedactionMode_REDACTION_MODE_APPLY, RedactionWindow{})
			require.NoError(t, err)
			require.True(t, rewrote)
			require.Equal(t, 1, found, "count traces, not rules")
			require.NotNil(t, replacement)
			require.Equal(t, int64(2), replacement.TotalObjects)
			block, err := encoding.OpenBlock(replacement, rw.r)
			require.NoError(t, err)
			result, err := block.FindTraceByID(ctx, idMatch, common.DefaultSearchOptions())
			require.NoError(t, err)
			batch := result.Trace.ResourceSpans[0]
			require.Equal(t, common.RedactedAttributeValue, attributeValue(batch.Resource.Attributes, "enc.alpha"))
			require.False(t, hasAttribute(batch.Resource.Attributes, "bi.alpha"))
			require.Equal(t, "enc:v1:"+otherKid+":resource-untouched", attributeValue(batch.Resource.Attributes, "enc.beta"))
			require.True(t, hasAttribute(batch.Resource.Attributes, "bi.beta"))
			attrs := batch.ScopeSpans[0].Spans[0].Attributes
			require.Equal(t, common.RedactedAttributeValue, attributeValue(attrs, "enc.beta"))
			require.False(t, hasAttribute(attrs, "bi.beta"))
			require.Equal(t, "enc:v1:"+kid+":span-untouched", attributeValue(attrs, "enc.alpha"))
			require.True(t, hasAttribute(attrs, "bi.alpha"))
			keep, err := block.FindTraceByID(ctx, idKeep, common.DefaultSearchOptions())
			require.NoError(t, err)
			require.Equal(t, "enc:v1:"+kid+"0:wrong-key", attributeValue(keep.Trace.ResourceSpans[0].Resource.Attributes, "enc.alpha"))
			require.True(t, hasAttribute(keep.Trace.ResourceSpans[0].Resource.Attributes, "bi.alpha"))
			require.True(t, hasAttribute(keep.Trace.ResourceSpans[0].ScopeSpans[0].Spans[0].Attributes, "bi.beta"))
			rewrote, found, replacement, err = c.RedactBlockAttributePairs(ctx, replacement, testTenantID, rules, tempopb.RedactionMode_REDACTION_MODE_APPLY, RedactionWindow{})
			require.NoError(t, err)
			require.False(t, rewrote)
			require.Zero(t, found)
			require.Nil(t, replacement)
		})
	}
}

func TestRedactBlockAttributePairsRejectsInvalidPairs(t *testing.T) {
	r, _, c, _ := testConfig(t, 0)
	rw := r.(*readerWriter)
	id := make([]byte, 16)
	id[15] = 1
	meta := attributeTestBlock(t, rw, vparquet5.VersionString, nil, []testData{{id: id, t: traceWithResourceAttr(id, "enc.secret", "safe")}})
	kid := strings.Repeat("a", 32)
	good := &tempopb.AttributeRedaction{Key: "resource.enc.secret", ValuePrefix: "enc:v1:" + kid}
	sidecar := &tempopb.AttributeRedaction{Key: "resource.bi.secret", ValuePrefix: "bi:v1:" + kid}
	for name, rules := range map[string][]*tempopb.AttributeRedaction{
		"empty":       nil,
		"odd":         {good},
		"wrong-scope": {good, {Key: "span.bi.secret", ValuePrefix: sidecar.ValuePrefix}},
		"wrong-name":  {good, {Key: "resource.bi.other", ValuePrefix: sidecar.ValuePrefix}},
		"wrong-kid":   {good, {Key: sidecar.Key, ValuePrefix: "bi:v1:" + strings.Repeat("b", 32)}},
		"short-kid":   {{Key: good.Key, ValuePrefix: "enc:v1:abc"}, {Key: sidecar.Key, ValuePrefix: "bi:v1:abc"}},
		"duplicate":   {good, sidecar, good, sidecar},
		"too-many":    append(make([]*tempopb.AttributeRedaction, 64), good, sidecar),
	} {
		t.Run(name, func(t *testing.T) {
			rewrote, found, out, err := c.RedactBlockAttributePairs(context.Background(), meta, testTenantID, rules, tempopb.RedactionMode_REDACTION_MODE_APPLY, RedactionWindow{})
			require.Error(t, err)
			require.False(t, rewrote)
			require.Zero(t, found)
			require.Nil(t, out)
		})
	}
}

func TestRedactBlockAttributesRejectsInvalidRulesAndNoMatch(t *testing.T) {
	r, _, c, _ := testConfig(t, 0)
	rw := r.(*readerWriter)
	id := make([]byte, 16)
	id[15] = 1
	meta := attributeTestBlock(t, rw, vparquet5.VersionString, nil, []testData{{id: id, t: traceWithResourceAttr(id, "enc.secret", "safe")}})
	for _, rule := range []*tempopb.AttributeRedaction{nil, {}, {Key: "span.enc.secret"}, {Key: "enc.secret", ValuePrefix: secretPrefix}, {Key: "event.enc.secret", ValuePrefix: secretPrefix}, {Key: "span.", ValuePrefix: secretPrefix}} {
		rewrote, found, out, err := c.RedactBlockAttributes(context.Background(), meta, testTenantID, rule, tempopb.RedactionMode_REDACTION_MODE_APPLY, RedactionWindow{})
		require.Error(t, err)
		require.False(t, rewrote)
		require.Zero(t, found)
		require.Nil(t, out)
	}
	rewrote, found, out, err := c.RedactBlockAttributes(context.Background(), meta, testTenantID, &tempopb.AttributeRedaction{Key: "resource.enc.secret", ValuePrefix: secretPrefix}, tempopb.RedactionMode_REDACTION_MODE_APPLY, RedactionWindow{})
	require.NoError(t, err)
	require.False(t, rewrote)
	require.Zero(t, found)
	require.Nil(t, out)
}

func TestRedactBlockAttributesHonorsWindow(t *testing.T) {
	for _, version := range []string{vparquet3.VersionString, vparquet4.VersionString, vparquet5.VersionString} {
		t.Run(version, func(t *testing.T) {
			r, _, c, _ := testConfig(t, 0)
			rw := r.(*readerWriter)
			now := uint64(time.Now().UnixNano())
			old := now - uint64(72*time.Hour)
			makeTrace := func(id []byte, start uint64) *tempopb.Trace {
				batch := test.MakeBatchWithAttributes(0, id, nil)
				span := test.MakeSpanWithTimeWindow(id, start, start+uint64(time.Second))
				span.Attributes = append(span.Attributes, attrString("enc.secret", secretPrefix+"payload"))
				batch.ScopeSpans = []*v1_trace.ScopeSpans{{Spans: []*v1_trace.Span{span}}}
				return &tempopb.Trace{ResourceSpans: []*v1_trace.ResourceSpans{batch}}
			}
			idNow := make([]byte, 16)
			idNow[15] = 1
			idOld := make([]byte, 16)
			idOld[15] = 2
			meta := attributeTestBlock(t, rw, version, nil, []testData{
				{id: idNow, t: makeTrace(idNow, now)},
				{id: idOld, t: makeTrace(idOld, old)},
			})
			rule := &tempopb.AttributeRedaction{Key: "span.enc.secret", ValuePrefix: secretPrefix}
			window := RedactionWindow{StartNano: int64(now - uint64(time.Minute)), EndNano: int64(now + uint64(time.Minute))}
			rewrote, found, out, err := c.RedactBlockAttributes(context.Background(), meta, testTenantID, rule, tempopb.RedactionMode_REDACTION_MODE_APPLY, window)
			require.NoError(t, err)
			require.True(t, rewrote)
			require.Equal(t, 1, found)
			block, err := encoding.OpenBlock(out, rw.r)
			require.NoError(t, err)
			for _, tc := range []struct {
				id   common.ID
				want string
			}{{idNow, common.RedactedAttributeValue}, {idOld, secretPrefix + "payload"}} {
				result, err := block.FindTraceByID(context.Background(), tc.id, common.DefaultSearchOptions())
				require.NoError(t, err)
				require.NotNil(t, result.Trace)
				require.Equal(t, tc.want, attributeValue(result.Trace.ResourceSpans[0].ScopeSpans[0].Spans[0].Attributes, "enc.secret"))
			}
		})
	}
}

func TestRedactBlockAttributesServiceNameSearchMetadata(t *testing.T) {
	for _, version := range []string{vparquet3.VersionString, vparquet4.VersionString, vparquet5.VersionString} {
		t.Run(version, func(t *testing.T) {
			r, _, c, _ := testConfig(t, 0)
			rw := r.(*readerWriter)
			id := make([]byte, 16)
			id[15] = 3
			batch := test.MakeBatchWithAttributes(0, id, nil)
			for _, attr := range batch.Resource.Attributes {
				if attr.Key == "service.name" {
					attr.Value = attrString("service.name", secretPrefix+"service").Value
				}
			}
			now := uint64(time.Now().UnixNano())
			root := test.MakeSpanWithTimeWindow(id, now, now+uint64(time.Second))
			root.ParentSpanId = nil
			batch.ScopeSpans = []*v1_trace.ScopeSpans{{Spans: []*v1_trace.Span{root}}}
			meta := attributeTestBlock(t, rw, version, nil, []testData{{id: id, t: &tempopb.Trace{
				ResourceSpans: []*v1_trace.ResourceSpans{batch},
			}}})
			rewrote, found, out, err := c.RedactBlockAttributes(context.Background(), meta, testTenantID,
				&tempopb.AttributeRedaction{Key: "resource.service.name", ValuePrefix: secretPrefix},
				tempopb.RedactionMode_REDACTION_MODE_APPLY, RedactionWindow{})
			require.NoError(t, err)
			require.True(t, rewrote)
			require.Equal(t, 1, found)
			block, err := encoding.OpenBlock(out, rw.r)
			require.NoError(t, err)
			trace, err := block.FindTraceByID(context.Background(), id, common.DefaultSearchOptions())
			require.NoError(t, err)
			require.Equal(t, common.RedactedAttributeValue, attributeValue(trace.Trace.ResourceSpans[0].Resource.Attributes, "service.name"))
			require.Len(t, survivingTraceIDs(t, r, out, "{ rootServiceName = `[REDACTED]` }"), 1)
			require.Empty(t, survivingTraceIDs(t, r, out, "{ rootServiceName = `"+secretPrefix+"service` }"))
		})
	}
}

// A public trace-by-ID read must not return the pre-redaction block during the
// compacted-block lookback. Reading the new block directly misses this leak:
// Find also searches recently compacted blocks after a blocklist poll.
func TestRedactBlockAttributesFindDoesNotExposeOriginalValue(t *testing.T) {
	r, _, c, _ := testConfig(t, time.Hour)
	rw := r.(*readerWriter)
	ctx, cancel := context.WithCancel(context.Background())
	r.EnablePolling(ctx, &mockJobSharder{}, false)
	t.Cleanup(func() {
		cancel()
		r.Shutdown()
	})

	id := make([]byte, 16)
	id[15] = 1
	const prefix = "enc:v1:630dcd2966c4336691125448bbb25b4f:"
	batch := test.MakeBatchWithAttributes(1, id, nil)
	batch.ScopeSpans[0].Spans[0].Attributes = append(batch.ScopeSpans[0].Spans[0].Attributes,
		attrString("enc.api.token", prefix+"payload"))
	meta := attributeTestBlock(t, rw, encoding.DefaultEncoding().Version(), nil, []testData{
		{id: id, t: &tempopb.Trace{ResourceSpans: []*v1_trace.ResourceSpans{batch}}},
	})
	r.PollNow(ctx)
	require.Len(t, r.BlockMetas(testTenantID), 1)

	rewrote, found, replacement, err := c.RedactBlockAttributes(ctx, meta, testTenantID,
		&tempopb.AttributeRedaction{Key: "span.enc.api.token", ValuePrefix: prefix},
		tempopb.RedactionMode_REDACTION_MODE_APPLY, RedactionWindow{})
	require.NoError(t, err)
	require.True(t, rewrote)
	require.Equal(t, 1, found)
	require.NotNil(t, replacement)
	metas := r.BlockMetas(testTenantID)
	require.Len(t, metas, 1)
	require.Equal(t, replacement.BlockID, metas[0].BlockID)
	live, compacted, err := r.BlockMeta(ctx, testTenantID, meta.BlockID)
	require.NoError(t, err)
	require.Nil(t, live)
	require.NotNil(t, compacted)
	require.True(t, compacted.RedactionSource, "the durable compacted source must not be eligible for trace lookback")
	// The worker's store must stop serving the source block as soon as the
	// replacement is committed, without waiting for the next blocklist poll.
	assertSanitized := func() {
		results, blockErrs, err := r.Find(ctx, testTenantID, id, BlockIDMin, BlockIDMax,
			time.Time{}, time.Time{}, common.DefaultSearchOptions())
		require.NoError(t, err)
		require.Empty(t, blockErrs)
		require.NotEmpty(t, results)
		for _, result := range results {
			value := attributeValue(result.Trace.ResourceSpans[0].ScopeSpans[0].Spans[0].Attributes, "enc.api.token")
			require.False(t, strings.HasPrefix(value, prefix), "trace-by-ID returned an unsanitized copy")
			require.Equal(t, common.RedactedAttributeValue, value)
		}
	}
	assertSanitized()
	r.PollNow(ctx)

	assertSanitized()
}

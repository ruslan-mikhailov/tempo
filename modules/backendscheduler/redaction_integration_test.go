package backendscheduler

import (
	"context"
	"flag"
	"testing"
	"time"

	"github.com/gogo/status"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"github.com/grafana/tempo/v3/modules/overrides"
	"github.com/grafana/tempo/v3/modules/storage"
	"github.com/grafana/tempo/v3/pkg/model"
	"github.com/grafana/tempo/v3/pkg/tempopb"
	v1_common "github.com/grafana/tempo/v3/pkg/tempopb/common/v1"
	v1_trace "github.com/grafana/tempo/v3/pkg/tempopb/trace/v1"
	"github.com/grafana/tempo/v3/pkg/util/test"
	"github.com/grafana/tempo/v3/tempodb"
	"github.com/grafana/tempo/v3/tempodb/backend"
	"github.com/grafana/tempo/v3/tempodb/encoding/common"
)

// traceWithNamespace builds a trace whose resource carries a specific namespace attribute,
// so a TraceQL query can select it.
func traceWithNamespace(id common.ID, ns string) *tempopb.Trace {
	attrs := []*v1_common.KeyValue{{
		Key:   "namespace",
		Value: &v1_common.AnyValue{Value: &v1_common.AnyValue_StringValue{StringValue: ns}},
	}}
	return &tempopb.Trace{ResourceSpans: []*v1_trace.ResourceSpans{test.MakeBatchWithAttributes(2, id, attrs)}}
}

// writeRealTenantBlock writes a complete block (with trace data, not just a meta) to the store's
// backend so it is discoverable via BlockMetas and readable by RedactBlock.
func writeRealTenantBlock(ctx context.Context, t *testing.T, store storage.Store, tenant string, traces []*tempopb.Trace, ids []common.ID) {
	t.Helper()
	dec := model.MustNewSegmentDecoder(model.CurrentEncoding)
	meta := &backend.BlockMeta{BlockID: backend.NewUUID(), TenantID: tenant}
	head, err := store.WAL().NewBlock(meta, model.CurrentEncoding)
	require.NoError(t, err)

	now := uint32(time.Now().Unix())
	for i, tr := range traces {
		b1, err := dec.PrepareForWrite(tr, 0, 0)
		require.NoError(t, err)
		b2, err := dec.ToObject([][]byte{b1})
		require.NoError(t, err)
		require.NoError(t, head.Append(ids[i], b2, now, now, true))
	}
	_, err = store.CompleteBlock(ctx, head)
	require.NoError(t, err)
}

// TestSubmitRedactionQueryEndToEnd exercises the query selector across the full submission →
// per-block execution path on real multi-block storage: a query submitted through the real API
// fans out one job per block, and executing those jobs redacts exactly the matching trace while
// leaving non-matching traces (and non-matching blocks) untouched.
//
// It drives the jobs the way Next() + the worker do — injecting the batch's selector into each
// job detail, then calling store.RedactBlock — rather than through the provider channel, which
// is non-deterministic in tests (the RedactionProvider goroutine races to drain pending jobs).
func TestSubmitRedactionQueryEndToEnd(t *testing.T) {
	cfg := Config{}
	cfg.RegisterFlagsAndApplyDefaults("", &flag.FlagSet{})
	tmpDir := t.TempDir()
	cfg.LocalWorkPath = tmpDir + "/work"

	ctx, cancel := context.WithCancel(context.Background())
	store, rr, ww := newStore(ctx, t, tmpDir)
	defer func() {
		cancel()
		store.Shutdown()
	}()

	limits, err := overrides.NewOverrides(overrides.Config{Defaults: overrides.Overrides{}}, nil, prometheus.NewRegistry())
	require.NoError(t, err)
	s, err := New(cfg, store, limits, rr, ww)
	require.NoError(t, err)

	tenant := "tenant-redact-e2e"
	idMatch := test.ValidTraceID(nil)
	idKeepA := test.ValidTraceID(nil)
	idKeepB := test.ValidTraceID(nil)

	// Block A holds the matching trace plus a keeper; block B holds only a keeper.
	writeRealTenantBlock(ctx, t, store, tenant,
		[]*tempopb.Trace{traceWithNamespace(idMatch, "secret"), traceWithNamespace(idKeepA, "keep")},
		[]common.ID{idMatch, idKeepA})
	writeRealTenantBlock(ctx, t, store, tenant,
		[]*tempopb.Trace{traceWithNamespace(idKeepB, "keep")},
		[]common.ID{idKeepB})

	require.Eventually(t, func() bool { return len(store.BlockMetas(tenant)) == 2 },
		3*time.Second, 50*time.Millisecond, "blocklist poll should discover both blocks")

	query := `{resource.namespace = "secret"}`
	resp, err := s.SubmitRedaction(user.InjectOrgID(ctx, tenant), &tempopb.SubmitRedactionRequest{
		Selector: &tempopb.SubmitRedactionRequest_Query{Query: &tempopb.TraceQLSelector{Query: query}},
	})
	require.NoError(t, err)
	require.EqualValues(t, 2, resp.JobsCreated, "one redaction job per block")

	batch := s.work.GetBatch(tenant)
	require.NotNil(t, batch)
	require.NotNil(t, batch.Query)
	require.Equal(t, query, batch.Query.Query)

	metaByID := make(map[string]*backend.BlockMeta)
	for _, m := range store.BlockMetas(tenant) {
		metaByID[m.BlockID.String()] = m
	}

	totalFound, rewroteBlocks := 0, 0
	for _, j := range s.work.ListAllPendingJobs() {
		require.Equal(t, tempopb.JobType_JOB_TYPE_REDACTION, j.GetType())
		rd := j.JobDetail.Redaction

		// Next() injects the batch selector; the worker forwards it to RedactBlock.
		rd.Query = batch.Query
		rd.Mode = batch.Mode

		meta := metaByID[rd.BlockId]
		require.NotNil(t, meta, "job references a discovered block")

		rewrote, found, _, err := store.RedactBlock(ctx, meta, tenant, nil, rd.Query.GetQuery(), rd.Mode, tempodb.RedactionWindow{StartNano: rd.StartTimeUnixNano, EndNano: rd.EndTimeUnixNano})
		require.NoError(t, err)
		totalFound += found
		if rewrote {
			rewroteBlocks++
		}
	}

	require.Equal(t, 1, totalFound, "exactly the one matching trace is selected across all blocks")
	require.Equal(t, 1, rewroteBlocks, "only the block containing the match is rewritten")
}

func TestSubmitAttributeRedactionPersistsAndDispatchesOneBatch(t *testing.T) {
	cfg := Config{}
	cfg.RegisterFlagsAndApplyDefaults("", &flag.FlagSet{})
	tmpDir := t.TempDir()
	cfg.LocalWorkPath = tmpDir + "/work"

	ctx, cancel := context.WithCancel(context.Background())
	store, rr, ww := newStore(ctx, t, tmpDir)
	defer func() {
		cancel()
		store.Shutdown()
	}()
	limits, err := overrides.NewOverrides(overrides.Config{Defaults: overrides.Overrides{}}, nil, prometheus.NewRegistry())
	require.NoError(t, err)
	s, err := New(cfg, store, limits, rr, ww)
	require.NoError(t, err)

	const tenant = "tenant-paired-redaction"
	writeTenantBlocks(ctx, t, backend.NewWriter(ww), tenant, 2)
	require.Eventually(t, func() bool { return len(store.BlockMetas(tenant)) == 2 }, 3*time.Second, 50*time.Millisecond)
	const kid = "0123456789abcdef0123456789abcdef"
	rules := []*tempopb.AttributeRedaction{
		{Key: "span.enc.secret", ValuePrefix: "enc:v1:" + kid},
		{Key: "span.bi.secret", ValuePrefix: "bi:v1:" + kid},
		{Key: "resource.enc.account", ValuePrefix: "enc:v1:" + kid},
		{Key: "resource.bi.account", ValuePrefix: "bi:v1:" + kid},
	}
	resp, err := s.SubmitAttributeRedaction(user.InjectOrgID(ctx, tenant), &tempopb.SubmitRedactionRequest{
		AttributeRedactions: rules,
		Mode:                tempopb.RedactionMode_REDACTION_MODE_DRY_RUN,
	})
	require.NoError(t, err)
	require.EqualValues(t, 2, resp.JobsCreated)
	require.Len(t, s.work.ListAllPendingJobs(), 2, "all pairs must share one job per block")
	require.Equal(t, rules, s.work.GetBatch(tenant).AttributeRedactions)
	require.Equal(t, resp.BatchId, s.work.GetBatch(tenant).BatchId)
	require.NoError(t, s.work.FlushBatchesToLocal(ctx, cfg.LocalWorkPath))
	require.NoError(t, s.work.FlushToLocal(ctx, cfg.LocalWorkPath, nil))

	reloaded, err := New(cfg, store, limits, rr, ww)
	require.NoError(t, err)
	require.NoError(t, reloaded.work.LoadFromLocal(ctx, cfg.LocalWorkPath))
	require.NoError(t, reloaded.work.LoadBatchesFromLocal(ctx, cfg.LocalWorkPath))
	require.Equal(t, rules, reloaded.work.GetBatch(tenant).AttributeRedactions)
	seen := make(map[string]bool)
	for range 2 {
		job := reloaded.work.NextPendingJob(tempopb.JobType_JOB_TYPE_REDACTION)
		require.NotNil(t, job)
		reloaded.mergedJobs <- job
		next, err := reloaded.Next(ctx, &tempopb.NextJobRequest{WorkerId: job.ID})
		require.NoError(t, err)
		require.Equal(t, resp.BatchId, next.Detail.BatchId)
		require.Nil(t, next.Detail.Redaction.AttributeRedaction)
		require.Equal(t, rules, next.Detail.Redaction.AttributeRedactions)
		require.Equal(t, tempopb.RedactionMode_REDACTION_MODE_DRY_RUN, next.Detail.Redaction.Mode)
		seen[next.Detail.Redaction.BlockId] = true
	}
	require.Len(t, seen, 2, "each block gets exactly one job with all pairs")
	require.Nil(t, reloaded.work.NextPendingJob(tempopb.JobType_JOB_TYPE_REDACTION))
}

func TestSubmitAttributeRedactionRejectsMalformedPairs(t *testing.T) {
	cfg := Config{}
	cfg.RegisterFlagsAndApplyDefaults("", &flag.FlagSet{})
	tmpDir := t.TempDir()
	cfg.LocalWorkPath = tmpDir + "/work"
	ctx, cancel := context.WithCancel(context.Background())
	store, rr, ww := newStore(ctx, t, tmpDir)
	defer func() {
		cancel()
		store.Shutdown()
	}()
	limits, err := overrides.NewOverrides(overrides.Config{Defaults: overrides.Overrides{}}, nil, prometheus.NewRegistry())
	require.NoError(t, err)
	s, err := New(cfg, store, limits, rr, ww)
	require.NoError(t, err)
	tenantCtx := user.InjectOrgID(ctx, "tenant-invalid-pairs")

	const kid = "0123456789abcdef0123456789abcdef"
	enc := &tempopb.AttributeRedaction{Key: "span.enc.secret", ValuePrefix: "enc:v1:" + kid}
	bi := &tempopb.AttributeRedaction{Key: "span.bi.secret", ValuePrefix: "bi:v1:" + kid}
	otherKid := &tempopb.AttributeRedaction{Key: bi.Key, ValuePrefix: "bi:v1:abcdef0123456789abcdef0123456789"}
	tooMany := make([]*tempopb.AttributeRedaction, 0, 66)
	for range 33 {
		tooMany = append(tooMany, enc, bi)
	}
	for _, tc := range []struct {
		name  string
		rules []*tempopb.AttributeRedaction
	}{
		{"empty", nil},
		{"odd count", []*tempopb.AttributeRedaction{enc}},
		{"wrong order", []*tempopb.AttributeRedaction{bi, enc}},
		{"wrong scope", []*tempopb.AttributeRedaction{enc, {Key: "resource.bi.secret", ValuePrefix: bi.ValuePrefix}}},
		{"wrong suffix", []*tempopb.AttributeRedaction{enc, {Key: "span.bi.other", ValuePrefix: bi.ValuePrefix}}},
		{"empty suffix", []*tempopb.AttributeRedaction{{Key: "span.enc.", ValuePrefix: enc.ValuePrefix}, {Key: "span.bi.", ValuePrefix: bi.ValuePrefix}}},
		{"mismatched kid", []*tempopb.AttributeRedaction{enc, otherKid}},
		{"uppercase kid", []*tempopb.AttributeRedaction{{Key: enc.Key, ValuePrefix: "enc:v1:ABCDEF0123456789abcdef0123456789"}, bi}},
		{"short kid", []*tempopb.AttributeRedaction{{Key: enc.Key, ValuePrefix: "enc:v1:a"}, bi}},
		{"nil rule", []*tempopb.AttributeRedaction{enc, nil}},
		{"duplicate pair", []*tempopb.AttributeRedaction{enc, bi, enc, bi}},
		{"too many", tooMany},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := s.SubmitAttributeRedaction(tenantCtx, &tempopb.SubmitRedactionRequest{AttributeRedactions: tc.rules})
			require.Equal(t, codes.InvalidArgument, status.Code(err))
			require.NotContains(t, err.Error(), kid, "validation must not expose token prefixes")
		})
	}
	_, err = s.SubmitAttributeRedaction(tenantCtx, &tempopb.SubmitRedactionRequest{
		AttributeRedactions: []*tempopb.AttributeRedaction{enc, bi},
		TraceIds:            [][]byte{[]byte("trace")},
	})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	_, err = s.SubmitAttributeRedaction(tenantCtx, &tempopb.SubmitRedactionRequest{
		AttributeRedactions: []*tempopb.AttributeRedaction{enc, bi},
		AttributeRedaction:  enc,
	})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	_, err = s.SubmitAttributeRedaction(tenantCtx, &tempopb.SubmitRedactionRequest{
		AttributeRedactions: []*tempopb.AttributeRedaction{enc, bi},
		Selector:            &tempopb.SubmitRedactionRequest_Query{Query: &tempopb.TraceQLSelector{Query: `{span.name = "secret"}`}},
	})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	_, err = s.SubmitRedaction(tenantCtx, &tempopb.SubmitRedactionRequest{AttributeRedactions: []*tempopb.AttributeRedaction{enc, bi}})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

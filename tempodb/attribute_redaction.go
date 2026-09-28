package tempodb

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"

	"github.com/grafana/tempo/v3/pkg/tempopb"
	"github.com/grafana/tempo/v3/tempodb/backend"
	"github.com/grafana/tempo/v3/tempodb/encoding"
	"github.com/grafana/tempo/v3/tempodb/encoding/common"
	"github.com/grafana/tempo/v3/tempodb/encoding/vparquet3"
	"github.com/grafana/tempo/v3/tempodb/encoding/vparquet4"
	"github.com/grafana/tempo/v3/tempodb/encoding/vparquet5"
)

// RedactBlockAttributes replaces matching string attributes without removing traces.
// A read-only scan runs first so a dry-run and a block with no matches never write.
// On error, found is zero and no replacement metadata is returned.
func (rw *readerWriter) RedactBlockAttributes(ctx context.Context, meta *backend.BlockMeta, tenantID string, rule *tempopb.AttributeRedaction, mode tempopb.RedactionMode, window RedactionWindow) (bool, int, *backend.BlockMeta, error) {
	if meta == nil || tenantID == "" || meta.TenantID != tenantID {
		return false, 0, nil, fmt.Errorf("invalid block metadata or tenant for attribute redaction")
	}
	if rule == nil || rule.ValuePrefix == "" || rule.Key == "" {
		return false, 0, nil, fmt.Errorf("attribute redaction requires a key and nonempty value prefix")
	}
	if mode != tempopb.RedactionMode_REDACTION_MODE_APPLY && mode != tempopb.RedactionMode_REDACTION_MODE_DRY_RUN {
		return false, 0, nil, fmt.Errorf("unsupported attribute redaction mode %d", mode)
	}
	if err := window.Validate(); err != nil {
		return false, 0, nil, fmt.Errorf("attribute redaction window: %w", err)
	}
	parsed := &common.AttributeRedactionRule{Prefix: rule.ValuePrefix}
	switch {
	case strings.HasPrefix(rule.Key, "span."):
		parsed.Scope = backend.DedicatedColumnScopeSpan
		parsed.Key = strings.TrimPrefix(rule.Key, "span.")
	case strings.HasPrefix(rule.Key, "resource."):
		parsed.Scope = backend.DedicatedColumnScopeResource
		parsed.Key = strings.TrimPrefix(rule.Key, "resource.")
	default:
		return false, 0, nil, fmt.Errorf("attribute redaction key %q must use span. or resource. scope", rule.Key)
	}
	if strings.TrimSpace(parsed.Key) == "" {
		return false, 0, nil, fmt.Errorf("attribute redaction key %q has an empty attribute name", rule.Key)
	}
	if start, end, ok := window.fetchBounds(); ok {
		parsed.StartNano, parsed.EndNano = start, end
	}

	// Deprecated vParquet3 is readable and has a compactor for this rewrite even
	// though it is not selected for ordinary periodic compaction or new writes.
	switch meta.Version {
	case vparquet3.VersionString, vparquet4.VersionString, vparquet5.VersionString:
	default:
		return false, 0, nil, fmt.Errorf("unsupported block version %q for attribute redaction", meta.Version)
	}
	enc, err := encoding.FromVersion(meta.Version)
	if err != nil {
		return false, 0, nil, err
	}
	opts := common.CompactionOptions{
		BlockConfig: common.BlockConfig{
			BloomFP:             common.DefaultBloomFP,
			BloomShardSizeBytes: common.DefaultBloomShardSizeBytes,
			Version:             meta.Version,
			RowGroupSizeBytes:   100_000_000,
			DedicatedColumns:    meta.DedicatedColumns,
		},
		OutputBlocks:               1,
		AttributeRedaction:         parsed,
		AttributeRedactionScanOnly: true,
		AttributeRedacted:          func() {},
		BytesWritten:               func(_, _ int) {},
		ObjectsCombined:            func(_, _ int) {},
		ObjectsWritten:             func(_, _ int) {},
		SpansDiscarded:             func(_, _, _ string, _ int) {},
		DisconnectedTrace:          func() {},
		RootlessTrace:              func() {},
		DedupedSpans:               func(_, _ int) {},
	}
	found := 0
	opts.AttributeRedacted = func() { found++ }
	_, err = enc.NewCompactor(opts).Compact(ctx, rw.logger, rw.r, rw.w, []*backend.BlockMeta{meta})
	if err != nil {
		return false, 0, nil, fmt.Errorf("scanning block %s for attribute redaction: %w", meta.BlockID, err)
	}
	if found == 0 || mode == tempopb.RedactionMode_REDACTION_MODE_DRY_RUN {
		return false, found, nil, nil
	}

	opts.AttributeRedactionScanOnly = false
	opts.AttributeRedacted = nil // Counted by the read-only pass.
	out, err := enc.NewCompactor(opts).Compact(ctx, rw.logger, rw.r, rw.w, []*backend.BlockMeta{meta})
	if err != nil {
		return false, 0, nil, fmt.Errorf("rewriting block %s for attribute redaction: %w", meta.BlockID, err)
	}
	if len(out) != 1 {
		return false, 0, nil, fmt.Errorf("expected one replacement for block %s, got %d", meta.BlockID, len(out))
	}
	if err := rw.c.MarkBlockCompacted(uuid.UUID(meta.BlockID), tenantID); err != nil {
		return false, 0, nil, fmt.Errorf("marking block %s compacted: %w", meta.BlockID, err)
	}
	return true, found, out[0], nil
}

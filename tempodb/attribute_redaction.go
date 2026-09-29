package tempodb

import (
	"context"
	"fmt"
	"strings"

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
	if rule == nil || rule.ValuePrefix == "" || rule.Key == "" {
		return false, 0, nil, fmt.Errorf("attribute redaction requires a key and nonempty value prefix")
	}
	parsed, err := parseAttributeRedactionRule(rule)
	if err != nil {
		return false, 0, nil, err
	}
	return rw.redactBlockAttributeRules(ctx, meta, tenantID, parsed, mode, window)
}

// RedactBlockAttributePairs applies adjacent encryption and blind-index rules
// in one scan and at most one replacement block.
func (rw *readerWriter) RedactBlockAttributePairs(ctx context.Context, meta *backend.BlockMeta, tenantID string, rules []*tempopb.AttributeRedaction, mode tempopb.RedactionMode, window RedactionWindow) (bool, int, *backend.BlockMeta, error) {
	if len(rules) == 0 || len(rules)%2 != 0 || len(rules) > 64 {
		return false, 0, nil, fmt.Errorf("attribute redaction requires 1 to 32 adjacent pairs")
	}
	parsed := &common.AttributeRedactionRule{Pairs: make([]common.AttributeRedactionPair, 0, len(rules)/2)}
	seen := make(map[string]struct{}, len(rules)/2)
	for i := 0; i < len(rules); i += 2 {
		enc, bi := rules[i], rules[i+1]
		if enc == nil || bi == nil {
			return false, 0, nil, fmt.Errorf("attribute redaction pair %d is incomplete", i/2)
		}
		encRule, err := parseAttributeRedactionRule(enc)
		if err != nil {
			return false, 0, nil, err
		}
		biRule, err := parseAttributeRedactionRule(bi)
		if err != nil {
			return false, 0, nil, err
		}
		kid := strings.TrimPrefix(enc.ValuePrefix, "enc:v1:")
		if !strings.HasPrefix(encRule.Key, "enc.") || !strings.HasPrefix(biRule.Key, "bi.") ||
			strings.TrimPrefix(encRule.Key, "enc.") == "" ||
			strings.TrimPrefix(encRule.Key, "enc.") != strings.TrimPrefix(biRule.Key, "bi.") ||
			encRule.Scope != biRule.Scope || len(kid) != 32 || bi.ValuePrefix != "bi:v1:"+kid {
			return false, 0, nil, fmt.Errorf("invalid attribute redaction pair %d", i/2)
		}
		for _, c := range kid {
			if c < '0' || c > '9' && (c < 'a' || c > 'f') {
				return false, 0, nil, fmt.Errorf("invalid attribute redaction key ID in pair %d", i/2)
			}
		}
		if _, ok := seen[enc.Key]; ok {
			return false, 0, nil, fmt.Errorf("duplicate attribute redaction key in pair %d", i/2)
		}
		seen[enc.Key] = struct{}{}
		encRule.Prefix += ":" // Require the token separator after the complete key ID.
		parsed.Pairs = append(parsed.Pairs, common.AttributeRedactionPair{Enc: *encRule, BiKey: biRule.Key, BiPrefix: biRule.Prefix})
	}
	return rw.redactBlockAttributeRules(ctx, meta, tenantID, parsed, mode, window)
}

func parseAttributeRedactionRule(rule *tempopb.AttributeRedaction) (*common.AttributeRedactionRule, error) {
	if rule == nil || rule.ValuePrefix == "" || rule.Key == "" {
		return nil, fmt.Errorf("attribute redaction requires a key and nonempty value prefix")
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
		return nil, fmt.Errorf("attribute redaction key %q must use span. or resource. scope", rule.Key)
	}
	if strings.TrimSpace(parsed.Key) == "" {
		return nil, fmt.Errorf("attribute redaction key %q has an empty attribute name", rule.Key)
	}
	return parsed, nil
}

func (rw *readerWriter) redactBlockAttributeRules(ctx context.Context, meta *backend.BlockMeta, tenantID string, parsed *common.AttributeRedactionRule, mode tempopb.RedactionMode, window RedactionWindow) (bool, int, *backend.BlockMeta, error) {
	if meta == nil || tenantID == "" || meta.TenantID != tenantID {
		return false, 0, nil, fmt.Errorf("invalid block metadata or tenant for attribute redaction")
	}
	if mode != tempopb.RedactionMode_REDACTION_MODE_APPLY && mode != tempopb.RedactionMode_REDACTION_MODE_DRY_RUN {
		return false, 0, nil, fmt.Errorf("unsupported attribute redaction mode %d", mode)
	}
	if err := window.Validate(); err != nil {
		return false, 0, nil, fmt.Errorf("attribute redaction window: %w", err)
	}
	if start, end, ok := window.fetchBounds(); ok {
		parsed.StartNano, parsed.EndNano = start, end
	}
	pairs := parsed.Pairs
	if len(pairs) == 0 {
		if sidecar, ok := parsed.LegacySidecar(); ok {
			pairs = []common.AttributeRedactionPair{sidecar}
		}
	}
	for _, pair := range pairs {
		for _, col := range meta.DedicatedColumns {
			if col.Scope == pair.Enc.Scope && col.Name == pair.BiKey {
				return false, 0, nil, fmt.Errorf("dedicated blind-index sidecar columns are unsupported for attribute redaction")
			}
		}
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
	if err := rw.markRedactionSourceCompacted(ctx, meta, out[0], tenantID); err != nil {
		return false, 0, nil, err
	}
	return true, found, out[0], nil
}

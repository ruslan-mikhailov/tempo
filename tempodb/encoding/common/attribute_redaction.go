package common

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/grafana/tempo/v3/tempodb/backend"
	"github.com/parquet-go/parquet-go"
)

const RedactedAttributeValue = "[REDACTED]"

// AttributeRedactionRule is a validated scope-qualified string replacement rule.
// StartNano/EndNano are both zero for an unbounded scan, otherwise both are set.
type AttributeRedactionRule struct {
	Scope              backend.DedicatedColumnScope
	Key, Prefix        string
	StartNano, EndNano uint64
	// Pairs are applied together to each trace in a single compaction pass.
	Pairs []AttributeRedactionPair
}

// AttributeRedactionPair removes BiKey only in scopes where Enc matched.
type AttributeRedactionPair struct {
	Enc      AttributeRedactionRule
	BiKey    string
	BiPrefix string
}

// MatchesSidecar requires the delimiter after the key ID; a prefix of another
// key ID must not be treated as the same key.
func (p *AttributeRedactionPair) MatchesSidecar(s string) bool {
	return strings.HasPrefix(s, p.BiPrefix+":")
}

// LegacySidecar preserves the permissive single-rule prefix while deriving its
// matching sidecar key ID. Unrelated legacy rules have no sidecar.
func (r *AttributeRedactionRule) LegacySidecar() (AttributeRedactionPair, bool) {
	if !strings.HasPrefix(r.Key, "enc.") || !strings.HasPrefix(r.Prefix, "enc:v1:") {
		return AttributeRedactionPair{}, false
	}
	kid := strings.TrimSuffix(strings.TrimPrefix(r.Prefix, "enc:v1:"), ":")
	if kid == "" || strings.Contains(kid, ":") {
		return AttributeRedactionPair{}, false
	}
	return AttributeRedactionPair{Enc: *r, BiKey: "bi." + strings.TrimPrefix(r.Key, "enc."), BiPrefix: "bi:v1:" + kid}, true
}

// RedactPairAttributes changes encryption values and removes the paired sidecar
// only when that same resource or span contains a matching encryption value.
// The generic representation differs across parquet versions; callers supply
// their value accessors without converting the rest of the trace.
func RedactPairAttributes[T any](attrs []T, pair *AttributeRedactionPair, key func(*T) string, encValue func(*T) string, sidecarMatches func(*T, *AttributeRedactionPair) bool, redact func(*T), apply, matched bool) ([]T, bool) {
	for i := range attrs {
		a := &attrs[i]
		if key(a) == pair.Enc.Key && pair.Enc.MatchesValue(encValue(a)) {
			matched = true
			if apply {
				redact(a)
			}
		}
	}
	if !matched || !apply {
		return attrs, matched
	}
	kept := attrs[:0]
	for i := range attrs {
		a := &attrs[i]
		if key(a) != pair.BiKey || !sidecarMatches(a, pair) {
			kept = append(kept, attrs[i])
		}
	}
	return kept, matched
}

func (r *AttributeRedactionRule) MatchesTime(start, end uint64) bool {
	return r.StartNano == 0 || (start <= r.EndNano && end >= r.StartNano)
}

// RowMightMatch is a conservative prefilter: a matching string must be stored
// as a byte-array value. It avoids reconstructing unrelated traces.
func (r *AttributeRedactionRule) RowMightMatch(row parquet.Row) bool {
	for _, value := range row {
		if value.Kind() != parquet.ByteArray {
			continue
		}
		s := string(value.ByteArray())
		if len(r.Pairs) == 0 && strings.HasPrefix(s, r.Prefix) {
			return true
		}
		for i := range r.Pairs {
			if r.Pairs[i].Enc.MatchesValue(s) {
				return true
			}
		}
	}
	return false
}

func (r *AttributeRedactionRule) MatchesValue(s string) bool {
	return strings.HasPrefix(s, r.Prefix) && s != RedactedAttributeValue
}

// RedactDedicatedString handles the numbered spare columns of all three parquet versions.
// The spare column number is the position among string columns in the same scope,
// not the position among all dedicated columns. The v3/v4 fields are *string;
// v5 fields are []string (including single-valued strings).
func (r *AttributeRedactionRule) RedactDedicatedString(attrs any, columns backend.DedicatedColumns, apply bool) (bool, error) {
	index := 0
	for _, col := range columns {
		if col.Scope != r.Scope || col.Type != backend.DedicatedColumnTypeString {
			continue
		}
		index++
		if col.Name != r.Key {
			continue
		}
		for _, option := range col.Options {
			if option == backend.DedicatedColumnOptionArray {
				return false, nil // an array is not a string value
			}
		}
		// All supported schemas place String01..StringNN first in
		// DedicatedAttributes. Index directly to avoid constructing a field name
		// and allocating on every matching span.
		fields := reflect.ValueOf(attrs).Elem()
		if index > fields.NumField() || !strings.HasPrefix(fields.Type().Field(index-1).Name, "String") {
			return false, fmt.Errorf("dedicated string column %d is unsupported", index)
		}
		field := fields.Field(index - 1)
		switch field.Kind() {
		case reflect.Pointer:
			if field.IsNil() || !r.MatchesValue(field.Elem().String()) {
				return false, nil
			}
			if apply {
				field.Elem().SetString(RedactedAttributeValue)
			}
			return true, nil
		case reflect.Slice:
			if field.Len() != 1 || !r.MatchesValue(field.Index(0).String()) {
				return false, nil
			}
			if apply {
				field.Index(0).SetString(RedactedAttributeValue)
			}
			return true, nil
		default:
			return false, fmt.Errorf("unsupported dedicated string column type %s", field.Type())
		}
	}
	return false, nil
}

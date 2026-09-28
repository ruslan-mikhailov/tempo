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
}

func (r *AttributeRedactionRule) MatchesTime(start, end uint64) bool {
	return r.StartNano == 0 || (start <= r.EndNano && end >= r.StartNano)
}

// RowMightMatch is a conservative prefilter: a matching string must be stored
// as a byte-array value. It avoids reconstructing unrelated traces.
func (r *AttributeRedactionRule) RowMightMatch(row parquet.Row) bool {
	for _, value := range row {
		if value.Kind() == parquet.ByteArray && strings.HasPrefix(string(value.ByteArray()), r.Prefix) {
			return true
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

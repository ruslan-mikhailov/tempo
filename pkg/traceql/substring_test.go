package traceql

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	substringTokenCoo = "bi:v1:630dcd2966c4336691125448bbb25b4f:E_S8rZC-kHLrifr_71XBBSP7w8jOWNCm2j6LHigypgM"
	substringTokenOol = "bi:v1:630dcd2966c4336691125448bbb25b4f:zQb64aCXnVL2KksfrDxrQwVtdw6Ljmwxv37So9E7ghc"
)

func substringQuery(tokens ...string) string {
	quoted := make([]string, len(tokens))
	for i, token := range tokens {
		quoted[i] = fmt.Sprintf("%q", token)
	}
	return `{span.bi.secret subarray_seq [` + strings.Join(quoted, ", ") + `]}`
}

func TestSubstringParserValidation(t *testing.T) {
	query := substringQuery(substringTokenCoo, substringTokenOol) + ` | select(span.enc.secret)`
	ast, err := Parse(query)
	require.NoError(t, err)
	require.NoError(t, ast.validate())
	require.Contains(t, ast.String(), `span.bi.secret subarray_seq ["`+substringTokenCoo+`", "`+substringTokenOol+`"]`)

	quotedField := strings.Replace(query, "span.bi.secret", `span."bi.secret"`, 1)
	quotedAST, err := Parse(quotedField)
	require.NoError(t, err)
	require.NoError(t, quotedAST.validate())
	require.Equal(t, ast.String(), quotedAST.String())
	roundTrip, err := Parse(ast.String())
	require.NoError(t, err)
	require.NoError(t, roundTrip.validate())
	require.Equal(t, ast.String(), roundTrip.String())
	maximumTokens := make([]string, 510)
	for i := range maximumTokens {
		maximumTokens[i] = substringTokenCoo
	}
	atLimit, err := Parse(substringQuery(maximumTokens...))
	require.NoError(t, err)
	require.NoError(t, atLimit.validate())
	negative, err := Parse(`{span."bi.secret" !subarray_seq ["` + substringTokenCoo + `"]}`)
	require.NoError(t, err)
	require.NoError(t, negative.validate())
	require.Contains(t, negative.String(), `span.bi.secret !subarray_seq ["`+substringTokenCoo+`"]`)
	negativeRoundTrip, err := Parse(negative.String())
	require.NoError(t, err)
	require.NoError(t, negativeRoundTrip.validate())

	for _, tc := range []struct {
		name  string
		query string
	}{
		{"no scope", strings.Replace(query, "span.bi.secret", ".bi.secret", 1)},
		{"resource scope", strings.Replace(query, "span.bi.secret", "resource.bi.secret", 1)},
		{"parent scope", strings.Replace(query, "span.bi.secret", "parent.span.bi.secret", 1)},
		{"ordinary field", strings.Replace(query, "span.bi.secret", "span.secret", 1)},
		{"encrypted field", strings.Replace(query, "span.bi.secret", "span.enc.secret", 1)},
		{"empty field", strings.Replace(query, "span.bi.secret", `span."bi."`, 1)},
		{"empty tokens", `{span.bi.secret subarray_seq []}`},
		{"scalar operand", `{span.bi.secret subarray_seq "cool"}`},
		{"numeric operand", `{span.bi.secret subarray_seq [123]}`},
		{"mixed operands", `{span.bi.secret subarray_seq ["` + substringTokenCoo + `", 123]}`},
		{"bad digest", substringQuery(strings.TrimSuffix(substringTokenCoo, "M") + "-")},
		{"padded digest", substringQuery(substringTokenCoo + "=")},
		{"bad key id", substringQuery(strings.Replace(substringTokenCoo, "630d", "630D", 1))},
		{"different key ids", substringQuery(substringTokenCoo, strings.Replace(substringTokenOol, "630d", "730d", 1))},
		{"negative bad digest", `{span.bi.secret !subarray_seq ["` + substringTokenCoo + `="]}`},
		{"negative different key ids", `{span.bi.secret !subarray_seq ["` + substringTokenCoo + `", "` + strings.Replace(substringTokenOol, "630d", "730d", 1) + `"]}`},
		{"array with public operator", `{span.bi.secret @> ["` + substringTokenCoo + `"]}`},
		{"string with public operator", `{span.bi.secret @> "substring"}`},
		{"too many tokens", substringQuery(append(maximumTokens, substringTokenCoo)...)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ast, err := Parse(tc.query)
			if err == nil {
				require.Error(t, ast.validate())
			}
		})
	}
}

func TestSubstringSequenceEvaluation(t *testing.T) {
	field := NewScopedAttribute(AttributeScopeSpan, false, "bi.secret")
	query := substringQuery(substringTokenCoo, substringTokenCoo)
	for _, tc := range []struct {
		name     string
		query    string
		value    Static
		present  bool
		expected bool
	}{
		{"repeated contiguous", query, NewStaticStringArray([]string{substringTokenOol, substringTokenCoo, substringTokenCoo, substringTokenOol}), true, true},
		{"single matching gram", substringQuery(substringTokenCoo), NewStaticStringArray([]string{substringTokenCoo}), true, true},
		{"wrong order", substringQuery(substringTokenCoo, substringTokenOol), NewStaticStringArray([]string{substringTokenOol, substringTokenCoo}), true, false},
		{"noncontiguous", query, NewStaticStringArray([]string{substringTokenCoo, substringTokenOol, substringTokenCoo}), true, false},
		{"insufficient repetitions", query, NewStaticStringArray([]string{substringTokenCoo}), true, false},
		{"missing array", query, NewStaticNil(), false, false},
		{"scalar instead of array", query, NewStaticString(substringTokenCoo), true, false},
		{"negative match", `{span.bi.secret !subarray_seq ["` + substringTokenCoo + `"]}`, NewStaticStringArray([]string{substringTokenOol}), true, true},
		{"negative missing", `{span.bi.secret !subarray_seq ["` + substringTokenCoo + `"]}`, NewStaticNil(), false, false},
		{"negative match present", `{span.bi.secret !subarray_seq ["` + substringTokenCoo + `"]}`, NewStaticStringArray([]string{substringTokenCoo}), true, false},
		{"negative other key", `{span.bi.secret !subarray_seq ["` + substringTokenCoo + `"]}`, NewStaticStringArray([]string{strings.Replace(substringTokenOol, "630d", "730d", 1)}), true, false},
		{"negative mixed keys", `{span.bi.secret !subarray_seq ["` + substringTokenCoo + `"]}`, NewStaticStringArray([]string{substringTokenOol, strings.Replace(substringTokenOol, "630d", "730d", 1)}), true, false},
		{"negative empty", `{span.bi.secret !subarray_seq ["` + substringTokenCoo + `"]}`, NewStaticStringArray([]string{}), true, false},
		{"negative scalar", `{span.bi.secret !subarray_seq ["` + substringTokenCoo + `"]}`, NewStaticString(substringTokenOol), true, false},
		{"or second key", `{(span.bi.secret subarray_seq ["` + substringTokenCoo + `"]) || (span.bi.secret subarray_seq ["` + strings.Replace(substringTokenOol, "630d", "730d", 1) + `"])}`, NewStaticStringArray([]string{strings.Replace(substringTokenOol, "630d", "730d", 1)}), true, true},
		{"equality unchanged", `{span.bi.secret = "` + substringTokenCoo + `"}`, NewStaticString(substringTokenCoo), true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ast, err := Parse(tc.query)
			require.NoError(t, err)
			require.NoError(t, ast.validate())
			pipeline, ok := ast.SinglePipeline()
			require.True(t, ok)
			span := &mockSpan{attributes: map[Attribute]Static{}}
			if tc.present {
				span.attributes[field] = tc.value
			}
			result, err := pipeline.evaluate([]*Spanset{{Spans: []Span{span}}})
			require.NoError(t, err)
			require.Equal(t, tc.expected, len(result) == 1)
		})
	}
	ast, err := Parse(query)
	require.NoError(t, err)
	pipeline, ok := ast.SinglePipeline()
	require.True(t, ok)
	single := NewStaticStringArray([]string{substringTokenCoo})
	spans := []Span{
		&mockSpan{attributes: map[Attribute]Static{field: single}},
		&mockSpan{attributes: map[Attribute]Static{field: single}},
	}
	matches, err := pipeline.evaluate([]*Spanset{{Spans: spans}})
	require.NoError(t, err)
	require.Empty(t, matches, "tokens from separate spans must not form a sequence")
}

func TestSubstringConditionExtractionPolarity(t *testing.T) {
	cases := []struct {
		name          string
		query         string
		expectedOp    Operator
		allConditions bool
	}{
		{"positive", substringQuery(substringTokenCoo, substringTokenOol), OpContainsSequence, true},
		{"negative", `{span.bi.secret !subarray_seq ["` + substringTokenCoo + `"]}`, OpNotContainsSequence, true},
		{"compared false", `{(span.bi.secret subarray_seq ["` + substringTokenCoo + `"]) = false}`, OpNone, false},
		{"negative compared false", `{(span.bi.secret !subarray_seq ["` + substringTokenCoo + `"]) = false}`, OpNone, false},
		{"or", `{(span.bi.secret subarray_seq ["` + substringTokenCoo + `"]) || (span.bi.secret subarray_seq ["` + substringTokenOol + `"])}`, OpContainsSequence, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ast, err := Parse(tc.query)
			require.NoError(t, err)
			require.NoError(t, ast.validate())
			pipeline, ok := ast.SinglePipeline()
			require.True(t, ok)
			req := FetchSpansRequest{AllConditions: true}
			pipeline.extractConditions(&req)
			require.NotEmpty(t, req.Conditions)
			require.Equal(t, NewScopedAttribute(AttributeScopeSpan, false, "bi.secret"), req.Conditions[0].Attribute)
			require.Equal(t, tc.expectedOp, req.Conditions[0].Op)
			if tc.expectedOp == OpContainsSequence || tc.expectedOp == OpNotContainsSequence {
				want := 1
				if tc.name == "or" {
					want = 2
				}
				require.Len(t, req.Conditions, want)
				for _, cond := range req.Conditions {
					require.Len(t, cond.Operands, 1)
					require.Equal(t, TypeStringArray, cond.Operands[0].Type)
					tokens, ok := cond.Operands[0].StringArray()
					require.True(t, ok)
					if tc.name == "positive" {
						require.Equal(t, []string{substringTokenCoo, substringTokenOol}, tokens)
					}
				}
			} else {
				require.Empty(t, req.Conditions[0].Operands)
				if tc.name == "compared false" || tc.name == "negative compared false" {
					require.Len(t, req.Conditions, 2)
					require.Equal(t, Condition{Attribute: NewIntrinsic(IntrinsicSpanStartTime), Op: OpNone}, req.Conditions[1])
				} else {
					require.Len(t, req.Conditions, 1)
				}
			}
			require.Equal(t, tc.allConditions, req.AllConditions)
		})
	}
}

func TestLiteralSubstringEvaluation(t *testing.T) {
	for _, tc := range []struct {
		name, query string
		value       Static
		present     bool
		want        bool
	}{
		{"plain match", `{span.unencrypted @> "ool"}`, NewStaticString("cool"), true, true},
		{"plain miss", `{span.unencrypted @> "OO"}`, NewStaticString("cool"), true, false},
		{"negative miss", `{span.unencrypted !@> "OO"}`, NewStaticString("cool"), true, true},
		{"negative match", `{span.unencrypted !@> "ool"}`, NewStaticString("cool"), true, false},
		{"missing positive", `{span.unencrypted @> "x"}`, NewStaticNil(), false, false},
		{"missing negative", `{span.unencrypted !@> "x"}`, NewStaticNil(), false, false},
		{"wrong type positive", `{span.unencrypted @> "x"}`, NewStaticInt(1), true, false},
		{"wrong type negative", `{span.unencrypted !@> "x"}`, NewStaticInt(1), true, false},
		{"NFC haystack", `{span.unencrypted @> "é"}`, NewStaticString("Cafe\u0301"), true, true},
		{"NFC needle", `{span.unencrypted !@> "e\u0301"}`, NewStaticString("Café"), true, false},
		{"literal metacharacters", `{span.unencrypted @> ".*"}`, NewStaticString("c.*l"), true, true},
		{"ciphertext key prefix", `{span.enc.api.token @> "enc:v1:630dcd2966c4336691125448bbb25b4f"}`, NewStaticString("enc:v1:630dcd2966c4336691125448bbb25b4f:7aUwjY5fPtHvu_dUnzcxBJc6XQ"), true, true},
		{"ciphertext other key", `{span.enc.api.token @> "enc:v1:730dcd2966c4336691125448bbb25b4f"}`, NewStaticString("enc:v1:630dcd2966c4336691125448bbb25b4f:7aUwjY5fPtHvu_dUnzcxBJc6XQ"), true, false},
		{"ciphertext negative match", `{span.enc.api.token !@> "enc:v1"}`, NewStaticString("enc:v1:630dcd2966c4336691125448bbb25b4f:7aUwjY5fPtHvu_dUnzcxBJc6XQ"), true, false},
		{"ciphertext negative miss", `{span.enc.api.token !@> "enc:v1:730dcd2966c4336691125448bbb25b4f"}`, NewStaticString("enc:v1:630dcd2966c4336691125448bbb25b4f:7aUwjY5fPtHvu_dUnzcxBJc6XQ"), true, true},
		{"ciphertext does not decrypt", `{span.enc.api.token @> "abc"}`, NewStaticString("enc:v1:630dcd2966c4336691125448bbb25b4f:7aUwjY5fPtHvu_dUnzcxBJc6XQ"), true, false},
		{"ciphertext missing negative", `{span.enc.api.token !@> "enc:v1"}`, NewStaticNil(), false, false},
		{"ciphertext wrong type", `{span.enc.api.token @> "enc:v1"}`, NewStaticInt(1), true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ast, err := Parse(tc.query)
			require.NoError(t, err)
			require.NoError(t, ast.validate())
			pipeline, ok := ast.SinglePipeline()
			require.True(t, ok)
			req := FetchSpansRequest{AllConditions: true}
			pipeline.extractConditions(&req)
			require.Len(t, req.Conditions, 1)
			require.Equal(t, OpNone, req.Conditions[0].Op)
			span := &mockSpan{attributes: map[Attribute]Static{}}
			if tc.present {
				span.attributes[req.Conditions[0].Attribute] = tc.value
			}
			result, err := pipeline.evaluate([]*Spanset{{Spans: []Span{span}}})
			require.NoError(t, err)
			require.Equal(t, tc.want, len(result) == 1)
		})
	}
}

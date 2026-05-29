package traceql

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v2"
)

// stub mappers used to assert exact output. They wrap the input so it is easy to
// see what was rewritten, and are deterministic.
func stubAttr(name string) string {
	segments := strings.Split(name, ".")
	for i, seg := range segments {
		if seg != "" {
			segments[i] = "A_" + seg
		}
	}
	return strings.Join(segments, ".")
}

func stubValue(value string, isRegex bool) string {
	if isRegex {
		return "R_" + value
	}
	return "V_" + value
}

// TestAnonymizeValidQueries asserts that every valid (and unsupported, which
// still parses) example anonymizes into something that re-parses, and that
// anonymization is deterministic. This exercises every AST node type.
func TestAnonymizeValidQueries(t *testing.T) {
	b, err := os.ReadFile(testExamplesFile)
	require.NoError(t, err)

	queries := &TestQueries{}
	require.NoError(t, yaml.Unmarshal(b, queries))

	for _, set := range [][]string{queries.Valid, queries.Unsupported} {
		for _, q := range set {
			t.Run(q, func(t *testing.T) {
				out, err := Anonymize(q, stubAttr, stubValue)
				require.NoError(t, err)

				// Output must be valid TraceQL.
				_, err = ParseNoOptimizations(out)
				require.NoError(t, err, "anonymized query did not re-parse: %s", out)

				// Deterministic.
				out2, err := Anonymize(q, stubAttr, stubValue)
				require.NoError(t, err)
				require.Equal(t, out, out2)
			})
		}
	}
}

// TestAnonymizeParseFails asserts unparseable queries return an error.
func TestAnonymizeParseFails(t *testing.T) {
	b, err := os.ReadFile(testExamplesFile)
	require.NoError(t, err)

	queries := &TestQueries{}
	require.NoError(t, yaml.Unmarshal(b, queries))

	for _, q := range queries.ParseFails {
		t.Run(q, func(t *testing.T) {
			_, err := Anonymize(q, stubAttr, stubValue)
			require.Error(t, err)
		})
	}
}

func TestAnonymizeExactOutput(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		// Per-segment attribute name hashing + string value.
		{`{ .something.sub_smth="value123" }`, "{ .A_something.A_sub_smth = `V_value123` }"},
		{`{ .something.another_attr="value123" }`, "{ .A_something.A_another_attr = `V_value123` }"},
		// Scopes stay readable; name segments hashed.
		{`{ span.http.path = "x" }`, "{ span.A_http.A_path = `V_x` }"},
		{`{ resource.foo = "x" }`, "{ resource.A_foo = `V_x` }"},
		{`{ parent.span.foo = "x" }`, "{ parent.span.A_foo = `V_x` }"},
		// Intrinsics, numbers, durations, enums untouched.
		{`{ duration > 1s }`, "{ duration > 1s }"},
		{`{ status = error }`, "{ status = error }"},
		{`{ span.http.status_code = 500 }`, "{ span.A_http.A_status_code = 500 }"},
		// Regex routed through the regex value mapper.
		{`{ span.foo =~ "bar.*" }`, "{ span.A_foo =~ `R_bar.*` }"},
		{`{ span.foo !~ "bar.*" }`, "{ span.A_foo !~ `R_bar.*` }"},
		// select (spanset pipeline joins elements with "|", no spaces).
		{`{} | select(.a, .b)`, "{ true }|select(.A_a, .A_b)"},
		// metrics: by() and over_time(attr) (metrics stage uses " | ").
		{`{} | rate() by (span.http.path)`, "{ true } | rate()by(span.A_http.A_path)"},
		{`{} | quantile_over_time(.dur, 0.9) by (.region)`, "{ true } | quantile_over_time(.A_dur,0.90000)by(.A_region)"},
		// metrics math: both sub-queries rewritten.
		{`({ .a="x" } | rate()) / ({ .b="y" } | rate())`, "({ .A_a = `V_x` } | rate()) / ({ .A_b = `V_y` } | rate())"},
		// compare: inner spanset filter rewritten.
		{`{} | compare({ .a="x" })`, "{ true } | compare({ .A_a = `V_x` }}"},
		// aggregate over an attribute (scalar pipeline).
		{`{ true } | avg(.field) = 2`, "{ true }|(avg(.A_field)) = 2"},
	}

	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			got, err := Anonymize(tt.in, stubAttr, stubValue)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

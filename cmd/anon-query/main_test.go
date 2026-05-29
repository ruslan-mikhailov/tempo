package main

import (
	"regexp"
	"regexp/syntax"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHashDeterministicAndDistinct(t *testing.T) {
	h := newHasher(defaultSalt)

	a := h.hashString("something")
	require.Equal(t, a, h.hashString("something"), "hash must be deterministic")
	require.NotEqual(t, a, h.hashString("other"), "different inputs should differ")
	require.Len(t, a, hashLen)

	// Salt changes the output.
	require.NotEqual(t, a, newHasher("different-salt").hashString("something"))
}

func TestHashAttributeNamePerSegment(t *testing.T) {
	h := newHasher(defaultSalt)

	got := h.hashAttributeName("something.sub_smth")
	parts := strings.Split(got, ".")
	require.Len(t, parts, 2)

	// Shared segment hashes identically across names.
	other := h.hashAttributeName("something.another_attr")
	require.Equal(t, parts[0], strings.Split(other, ".")[0], "shared 'something' segment must match")
	require.NotEqual(t, parts[1], strings.Split(other, ".")[1], "sibling segments must differ")
}

func TestValueMapperRegexRouting(t *testing.T) {
	h := newHasher(defaultSalt)
	// Regex operands go through hashRegexValue, plain ones through hashString.
	require.Equal(t, h.hashRegexValue("x"), h.valueMapper("x", true))
	require.Equal(t, h.hashString("x"), h.valueMapper("x", false))
}

func TestHashRegexValue(t *testing.T) {
	h := newHasher(defaultSalt)

	t.Run("preserves structure, hashes literals", func(t *testing.T) {
		got := h.hashRegexValue("^a(some|bad).*")

		// Still a valid regex.
		_, err := regexp.Compile(got)
		require.NoError(t, err)

		// Literal text is gone.
		assert.NotContains(t, got, "some")
		assert.NotContains(t, got, "bad")

		// Structure survives. (Note: String() normalizes ^ to \A and . to a
		// flag-wrapped form, so we only check the operators that stay verbatim.)
		for _, want := range []string{"(", "|", ")", "*"} {
			assert.Contains(t, got, want, "structural %q should survive", want)
		}

		// Deterministic.
		assert.Equal(t, got, h.hashRegexValue("^a(some|bad).*"))
	})

	t.Run("shared literal runs hash identically", func(t *testing.T) {
		// Two separate "foo" runs (split by an any-char) must map to the same hash.
		got := h.hashRegexValue("foo.foo")
		fooHash := h.hashString("foo")
		assert.NotContains(t, got, "foo", "original literal must be gone")
		assert.Equal(t, 2, strings.Count(got, fooHash), "both runs hash to the same token")
	})

	t.Run("literal matches plain value hash (shared keyspace)", func(t *testing.T) {
		assert.Equal(t, h.hashString("some"), h.hashRegexValue("some"))
	})

	t.Run("no literals: only structural normalization, no hashing", func(t *testing.T) {
		for _, p := range []string{".*", "^$", "[a-z]+"} {
			re, err := syntax.Parse(p, syntax.Perl)
			require.NoError(t, err)
			assert.Equal(t, re.String(), h.hashRegexValue(p), "pattern %q has no literals to hash", p)
		}
	})

	t.Run("invalid regex falls back to whole-string hash", func(t *testing.T) {
		assert.Equal(t, h.hashString("["), h.hashRegexValue("["))
	})
}

func TestRunEndToEnd(t *testing.T) {
	in := strings.NewReader(strings.Join([]string{
		`{ .something.sub_smth="value123" }`,
		`{ .something.another_attr="value123" }`,
		``, // blank line skipped
		`not a valid query {`,
	}, "\n"))

	var out, errOut strings.Builder
	code := run(in, &out, &errOut)

	require.Equal(t, 1, code, "exit code should be non-zero when a line fails")

	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	require.Len(t, lines, 2, "only the two valid lines should be emitted")

	// "something" must hash to the same token on both lines.
	seg0 := func(line string) string {
		// {  .<seg0>.<seg1> = `...` }
		_, rest, _ := strings.Cut(line, ".")
		seg, _, _ := strings.Cut(rest, ".")
		return seg
	}
	assert.Equal(t, seg0(lines[0]), seg0(lines[1]))

	// The same value must hash identically on both lines.
	val := func(line string) string {
		return line[strings.Index(line, "`"):strings.LastIndex(line, "`")]
	}
	assert.Equal(t, val(lines[0]), val(lines[1]))

	// The invalid line is reported on stderr.
	assert.Contains(t, errOut.String(), "not a valid query {")
}

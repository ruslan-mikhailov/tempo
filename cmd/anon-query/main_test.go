package main

import (
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
	// Current behavior: regex and plain hashing are identical, but both are
	// deterministic. This guards the routing wiring.
	require.Equal(t, h.hashRegexValue("x"), h.valueMapper("x", true))
	require.Equal(t, h.hashString("x"), h.valueMapper("x", false))
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

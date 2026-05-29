// Command anon-query anonymizes TraceQL queries read from stdin and writes the
// anonymized queries to stdout, one per line:
//
//	cat input.txt | anon-query > output.txt
//
// Custom attribute name segments and string literal values are replaced with
// deterministic HMAC-SHA256 hashes. The same input always maps to the same
// hash, so a given attribute or value is consistent across all lines. Numbers,
// durations, booleans, enums, intrinsics, scopes and hints are left intact.
//
// Lines that fail to parse are reported on stderr and skipped; the process
// exits non-zero if any line failed.
package main

import (
	"bufio"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"os"
	"regexp/syntax"
	"strings"

	"github.com/grafana/tempo/pkg/traceql"
)

// defaultSalt is the HMAC key used for hashing. It can be overridden at runtime
// via the ANON_QUERY_SALT environment variable.
const defaultSalt = "REPLACEME"

const saltEnvVar = "ANON_QUERY_SALT"

// hashLen is the number of hex characters kept from each hash, trading a
// negligible collision risk for readability.
const hashLen = 10

// hasher reuses a single keyed HMAC across calls (Reset before each hash) to
// avoid re-initializing the SHA-256 state per token. This is safe because the
// tool processes input strictly sequentially on one goroutine.
type hasher struct {
	mac hash.Hash
}

func newHasher(salt string) *hasher {
	return &hasher{mac: hmac.New(sha256.New, []byte(salt))}
}

// hashString returns a deterministic, truncated hex HMAC-SHA256 of s.
func (h *hasher) hashString(s string) string {
	h.mac.Reset()
	h.mac.Write([]byte(s))
	sum := hex.EncodeToString(h.mac.Sum(nil))
	if len(sum) > hashLen {
		sum = sum[:hashLen]
	}
	return sum
}

// hashRegexValue handles operands of regex comparisons (=~ / !~). It hashes only
// the literal text inside the pattern while preserving its structure (anchors,
// groups, alternation, quantifiers, char classes, metacharacters). For example
// `^a(some|bad).*` keeps its shape, with `a`, `some` and `bad` each replaced by
// a hash. If the operand is not a valid regex, it falls back to hashing the
// whole string.
func (h *hasher) hashRegexValue(s string) string {
	// syntax.Perl is the flag set Go's own regexp.Compile parses with (it's Go's
	// default RE2 syntax, not actual Perl). TraceQL compiles regexes via stdlib
	// regexp too, so this accepts exactly the patterns TraceQL accepts.
	re, err := syntax.Parse(s, syntax.Perl)
	if err != nil {
		return h.hashString(s)
	}
	h.hashRegexLiterals(re)
	return re.String()
}

// hashRegexLiterals walks a parsed regex AST and replaces every literal run with
// its hash, in place. Non-literal nodes (anchors, groups, alternation,
// quantifiers, char classes, ...) are left untouched so the structure survives.
func (h *hasher) hashRegexLiterals(re *syntax.Regexp) {
	if re == nil {
		return
	}
	if re.Op == syntax.OpLiteral && len(re.Rune) > 0 {
		re.Rune = []rune(h.hashString(string(re.Rune)))
	}
	for _, sub := range re.Sub {
		h.hashRegexLiterals(sub)
	}
}

// hashAttributeName hashes each dot-separated segment of an attribute name
// independently and rejoins them, so shared prefixes (e.g. the "http" in
// .http.status_code and .http.method) map to the same token while siblings
// differ. Scopes (span./resource./...) are not part of the name and are handled
// by the parser, so they stay readable.
func (h *hasher) hashAttributeName(name string) string {
	segments := strings.Split(name, ".")
	for i, seg := range segments {
		if seg != "" {
			segments[i] = h.hashString(seg)
		}
	}
	return strings.Join(segments, ".")
}

func (h *hasher) valueMapper(value string, isRegex bool) string {
	if isRegex {
		return h.hashRegexValue(value)
	}
	return h.hashString(value)
}

func main() {
	os.Exit(run(os.Stdin, os.Stdout, os.Stderr))
}

func run(in io.Reader, out, errOut io.Writer) int {
	salt := defaultSalt
	if v, ok := os.LookupEnv(saltEnvVar); ok {
		salt = v
	}
	h := newHasher(salt)

	scanner := bufio.NewScanner(in)
	// Allow long queries (default token limit is 64KiB).
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)

	bufOut := bufio.NewWriter(out)
	defer bufOut.Flush()

	exitCode := 0
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		anon, err := traceql.Anonymize(line, h.hashAttributeName, h.valueMapper)
		if err != nil {
			fmt.Fprintf(errOut, "anon-query: %v: %s\n", err, line)
			exitCode = 1
			continue
		}
		fmt.Fprintln(bufOut, anon)
	}
	if err := scanner.Err(); err != nil {
		fmt.Fprintf(errOut, "anon-query: read error: %v\n", err)
		return 1
	}
	return exitCode
}

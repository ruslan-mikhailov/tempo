# anon-query

`anon-query` anonymizes TraceQL queries. It reads queries from stdin (one per
line) and writes the anonymized queries to stdout, replacing custom attribute
names and string values with deterministic hashes while keeping each query
valid, parseable TraceQL.

It handles both spanset queries and metrics queries.

## Usage

```bash
cat input.txt | anon-query > output.txt
```

Build it with:

```bash
go build ./cmd/anon-query
```

## Example

Input:

```
{ .something.sub_smth="value123" }
{ .something.another_attr="value123" }
```

Output (hashes are illustrative):

```
{ .a1b2c3d4e5.f6a7b8c9d0 = `1a2b3c4d5e` }
{ .a1b2c3d4e5.0f9e8d7c6b = `1a2b3c4d5e` }
```

Note that:

- `something` hashes to the same token on both lines.
- `value123` hashes to the same token on both lines.
- The sibling segments (`sub_smth` vs `another_attr`) differ.

The output is regenerated from the parsed query, so spacing is normalized and
string literals are quoted with backticks. Only the query's structure is
preserved, not its original formatting.

## What gets hashed

- **Custom attribute names**, hashed per dot-separated segment. For example
  `span.http.status_code` becomes `span.<hash(http)>.<hash(status_code)>`. The
  scope prefix (`span.`, `resource.`, `parent.`, `event.`, `link.`,
  `instrumentation.`, `trace:`, `span:`) stays readable.
- **String literal values** (e.g. `="value"`, `=~ "regex"`).

## What is left untouched

- Numbers, durations, booleans and enums (e.g. `> 500`, `> 1s`, `= true`,
  `= error`, `= client`).
- Intrinsics (`duration`, `status`, `name`, `kind`, `traceDuration`, …) and
  structural attributes.
- Scope prefixes (see above).
- Query hints, e.g. `with(sample=0.1)`.

## Hashing

Hashing is HMAC-SHA256 keyed by a salt, hex-encoded and truncated for
readability. Hashing is deterministic: the same input always produces the same
output, so attributes and values are consistent across all lines and across
runs (with the same salt).

The salt defaults to the constant `REPLACEME`. Override it for real
anonymization:

```bash
ANON_QUERY_SALT='your-private-salt' anon-query < input.txt > output.txt
```

## Regex values

Operands of regex comparisons (`=~`, `!~`) are currently hashed exactly like
plain strings. This is intentionally isolated in `hashRegexValue` so it can be
replaced later with a regex-aware strategy without affecting the rest of the
tool.

## Invalid queries

A query that fails to parse is reported on stderr (with the offending line) and
skipped; processing continues for the remaining lines. The process exits with a
non-zero status if any line failed.

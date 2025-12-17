# Translator

This module translate SQL queries into FetchSpansRequest or TraceQL.

> [!WARNING]
> An LLM was heavily used to produce this code.
> Although it was reviewed, the module is intended
> for demonstration purposes only and must not be used in production.

Docker image can be built by:

```bash
docker build -t sql-translator:latest .
```

The image will run a server that accepts SQL queries in `/convert?q=<query>`

To use it as a CLI took just pass a SQL query in input. Example:

```bash
cargo run --bin sql-to-fetchrequest -- "SELECT * FROM spans_view"
```

To translate to TraceQL:

```bash
cargo run --bin sql-to-traceql -- "SELECT * FROM spans_view"
```

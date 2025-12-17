# Translator

This module translate SQL queries into FetchSpansRequest.

> [!WARNING]
> An LLM was heavily used to produce this code.
> Although it was reviewed, the module is intended
> for demonstration purposes only and must not be used in production.

Docker image can be built by:

```bash
docker build -t sql-translator:latest .
```

To use it as a CLI took just pass a SQL query in input. Example:

```bash
cargo run -- "SELECT traceid, span_name FROM spans_view WHERE span_name='test' AND span_name='ttt'"
```

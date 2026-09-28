## Docker-compose

So you found your way to the docker compose examples?  This is a great place to
get started with Tempo and see some of the various configuration options

Refer to [getting-started](https://grafana.com/docs/tempo/latest/getting-started/docker-example/) for a walk-through using the single-binary example.

### Examples

The easiest example to start with is the [single-binary](./single-binary/). This example will run Tempo as a single binary, [xk6-client-tracing](https://github.com/grafana/xk6-client-tracing) 
to generate traces and Grafana to visualize trace data.

To use any example simply:

1. Navigate to the appropriate folder and run `docker-compose up`
1. Visit [Grafana Explore](http://localhost:3000/explore) and try some basic queries. See [the docs](https://grafana.com/docs/tempo/latest/traceql/construct-traceql-queries/) for help on more complex queries.  
  `{}` - basic search that finds everything  
  `{} | rate()` - rate of all spans
1. Visit [Traces Drilldown](http://localhost:3000/a/grafana-exploretraces-app/) for a queryless way to explore your data.
1. Connect your favorite LLM agent to our [MCP server](https://grafana.com/docs/tempo/latest/api_docs/mcp-server/)

### Features

See below for a list of all examples and the features they demonstrate

| Example | Deployment | Tenancy | Trace Ingestion | Storage | Other Features |
|---------|------------|---------|-----------------|---------|------------------|
| [Single Binary](./single-binary/) | Single binary | Single tenant | Alloy | Local filesystem | vulture for data integrity, metrics generator, streaming queries, mcp |
| [Distributed](./distributed/) | Distributed microservices | Single tenant | Alloy | S3 (MinIO) | vulture for data integrity, metrics-generator, streaming queries, mcp |
| [Multitenant](./multitenant/) | Single binary | Multitenant | OTel Collector + Direct OTLP | Local filesystem | vulture for data integrity, multiple tenants (tenant-1, tenant-2), streaming queries, mcp |
| [Debug](./debug/) | Single binary | Single tenant | Direct OTLP | Local filesystem | vulture for data integrity, tempo-debug image for breakpoint debugging, streaming queries, mcp | 

### Plaintext Loki logs in the distributed example

From `example/docker-compose/distributed`, run `docker compose up -d --build`.
The Grafana image builds `tempo-datasource` and `grafana-loki-datasource`
from their local checkouts on top of the same `grafana-base` image.
The `secret-logs` container posts fictional email addresses and `demo-` tokens
directly to the single-binary Loki service every five seconds. Select the
**Loki** datasource in [Grafana Explore](http://localhost:3000/explore), set
the time range to the last 15 minutes, and try:

```logql
{job="secret-logs-demo"}
{job="secret-logs-demo"} |= "api_token=demo-"
{job="secret-logs-demo", customer_email="alice@example.invalid"} |= "login"
```

This is deliberately **unencrypted** test data. The email is both an indexed
stream label and part of the log line, and the token is in the line. Loki's
`3100` port is also exposed locally for inspecting raw query responses. The
script bypasses Alloy; no protected-logs implementation is enabled.

### Build images (optional)

This step is not necessary, but it can be nice for local testing. The examples default to the published Tempo 3.0.0 images.
To use your local code when running the examples, build the local images from the project root folder and start Docker Compose from the example directory with `TEMPO_IMAGE_TAG=latest`:

```console
# From the project root.
make docker-images

# From the example directory.
TEMPO_IMAGE_TAG=latest docker compose up
```

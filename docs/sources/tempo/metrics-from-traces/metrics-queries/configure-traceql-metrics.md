---
aliases:
  - ../../operations/traceql-metrics/ # /docs/tempo/next/operations/traceql-metrics/
title: Configure TraceQL metrics
menuTitle: Configure TraceQL metrics
description: Learn about configuring TraceQL metrics.
weight: 400
keywords:
  - Prometheus
  - TraceQL
  - TraceQL metrics
---

# Configure TraceQL metrics

{{< docs/shared source="tempo" lookup="traceql-metrics-admonition.md" version="<TEMPO_VERSION>" >}}

TraceQL language provides metrics queries as an experimental feature.
Metric queries extend trace queries by applying a function to trace query results.
This powerful feature creates metrics from traces, much in the same way that LogQL metric queries create metrics from logs.

## Before you begin

To use the metrics generated from traces, you need to:

- Set the `local-blocks` processor to active in your `metrics-generator` configuration
- Configure a Tempo data source in Grafana or Grafana Cloud (documentation)
- Access Grafana Cloud or Grafana version 10.4 or later (documentation)

Refer to the [Metrics-generator configuration](http://grafana.com/docs/tempo/<TEMPO_VERSION>/configuration/#metrics-generator) documentation for more information about the `metrics-generator` configuration.

## Activate and configure the `local-blocks` processor

The local-blocks processor must be enabled to start using metrics queries like `{ } | rate()`.
If not enabled, then the metrics queries fail with the error `localblocks processor not found`.
Enabling the `local-blocks` processor can be done either per tenant or in all tenants.

To activate the `local-blocks` processor for all users, add it to the list of processors in the `overrides` block of your Tempo configuration.

```yaml
# Global overrides configuration.
overrides:
  metrics_generator_processors: ['local-blocks']
```

To configure the processor per tenant, use the `metrics_generator_processor` override.

Example for per-tenant in the per-tenant overrides:

```yaml
overrides:
  'tenantID':
    metrics_generator_processors:
      - local-blocks
```

By default, for all tenants in the main configuration:

```yaml
overrides:
  defaults:
    metrics_generator:
      processors: [local-blocks]
```

Add this configuration to run TraceQL metrics queries against all spans (and not just server spans):

```yaml
metrics_generator:
  processor:
    local_blocks:
      filter_server_spans: false
```

To run metrics queries on historical data, you must configure the local-blocks processor to flush RF1 blocks to object storage:

```yaml
metrics_generator:
  processor:
    local_blocks:
      flush_to_storage: true
```

Setting `flush_to_storage` to `true` ensures that metrics blocks are flushed to storage so TraceQL metrics queries against historical data.

If you configured Tempo using the `tempo-distributed` Helm chart, you can also set `traces_storage` using your `values.yaml` file.
Refer to the [Helm chart for an example](https://github.com/grafana/helm-charts/blob/559ecf4a9c9eefac4521454e7a8066778e4eeff7/charts/tempo-distributed/values.yaml#L362).

For more information about overrides, refer to [Standard overrides](https://grafana.com/docs/tempo/<TEMPO_VERSION>/configuration/#standard-overrides).

## Evaluate query timeouts

Because of their expensive nature, these queries can take a long time to run.
As such, consider increasing the timeouts in various places of
the system to allow enough time for the data to be returned.

Consider these areas when raising timeouts:

- Any proxy in front of Grafana
- Grafana data source for Prometheus pointing at Tempo
- Tempo configuration
  - `querier.search.query_timeout`
  - `server.http_server_read_timeout`
  - `server.http_server_write_timeout`

## Set TraceQL metrics query options

The `query_frontend.metrics` configuration block controls all TraceQL metrics queries.
The configuration depends on the environment.

{{< admonition type="note" >}}
The default maximum time range for a metrics query is 3 hours, configured using the `query_frontend.metrics.max_duration` parameter.

This is different to the default TraceQL maximum time range of 168 hours (7 days).

{{< /admonition >}}

For example, in a cloud environment, smaller jobs with more concurrency may be
desired due to the nature of scale on the backend.

```yaml
query_frontend:
  metrics:
    concurrent_jobs: 1000
    target_bytes_per_job: 2.25e+08 # ~225MB
    interval: 30m0s
```

For an on-prem backend, you can improve query times by lowering the concurrency,
while increasing the job size.

```yaml
query_frontend:
  metrics:
    concurrent_jobs: 8
    target_bytes_per_job: 1.25e+09 # ~1.25GB
```

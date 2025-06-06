---
title: Distributor refusing spans
description: Troubleshoot distributor refusing spans
weight: 471
aliases:
- ../../operations/troubleshooting/max-trace-limit-reached/ # https://grafana.com/docs/tempo/<TEMPO_VERSION>/operations/troubleshooting/max-trace-limit-reached/
- ../max-trace-limit-reached/ # https://grafana.com/docs/tempo/<TEMPO_VERSION>/troubleshooting/max-trace-limit-reached/
---

# Distributor refusing spans

The two most likely causes of refused spans are unhealthy ingesters or trace limits being exceeded.

To log spans that are discarded, add the `--distributor.log_discarded_spans.enabled` flag to the distributor or
adjust the [distributor configuration](https://grafana.com/docs/tempo/<TEMPO_VERSION>/configuration/#distributor):

```yaml
distributor:
  log_discarded_spans:
    enabled: true
    include_all_attributes: false # set to true for more verbose logs
```

Adding the flag logs all discarded spans, as shown below:

```
level=info ts=2024-08-19T16:06:25.880684385Z caller=distributor.go:767 msg=discarded spanid=c2ebe710d2e2ce7a traceid=bd63605778e3dbe935b05e6afd291006
level=info ts=2024-08-19T16:06:25.881169385Z caller=distributor.go:767 msg=discarded spanid=5352b0cb176679c8 traceid=ba41cae5089c9284e18bca08fbf10ca2
```

## Unhealthy ingesters

Unhealthy ingesters can be caused by failing OOMs or scale down events.
If you have unhealthy ingesters, your log line will look something like this:

```
msg="pusher failed to consume trace data" err="at least 2 live replicas required, could only find 1"
```

In this case, you may need to visit the ingester [ring page](https://grafana.com/docs/tempo/<TEMPO_VERSION>/operations/manage-advanced-systems/consistent_hash_ring/) at `/ingester/ring` on the Distributors
and "Forget" the unhealthy ingesters.
This works in the short term, but the long term fix is to stabilize your ingesters.

## Trace limits reached

In high volume tracing environments, the default trace limits are sometimes not sufficient.
These limits exist to protect Tempo and prevent it from OOMing, crashing, or otherwise allow tenants to not DOS each other.
If you are refusing spans due to limits, you'll see logs like this at the distributor:

```
msg="pusher failed to consume trace data" err="rpc error: code = FailedPrecondition desc = TRACE_TOO_LARGE: max size of trace (52428800) exceeded while adding 15632 bytes to trace a0fbd6f9ac5e2077d90a19551dd67b6f for tenant single-tenant"
msg="pusher failed to consume trace data" err="rpc error: code = FailedPrecondition desc = LIVE_TRACES_EXCEEDED: max live traces per tenant exceeded: per-user traces limit (local: 60000 global: 0 actual local: 60000) exceeded"
msg="pusher failed to consume trace data" err="rpc error: code = ResourceExhausted desc = RATE_LIMITED: ingestion rate limit (15000000 bytes) exceeded while adding 10 bytes"
```

You'll also see the following metric incremented. The `reason` label on this metric will contain information about the refused reason.

```
tempo_discarded_spans_total
```

In this case, use available configuration options to [increase limits](https://grafana.com/docs/tempo/<TEMPO_VERSION>/configuration/#ingestion-limits).

## Client resets connection

When the client resets the connection before the distributor can consume the trace data, you see logs like this:

```
msg="pusher failed to consume trace data" err="context canceled"
```

This issue needs to be fixed on the client side. To inspect which clients are causing the issue, logging discarded spans
with `include_all_attributes: true` may help.

Note that there may be other reasons for a closed context as well. Identifying the reason for a closed context is
not straightforward and may require additional debugging.
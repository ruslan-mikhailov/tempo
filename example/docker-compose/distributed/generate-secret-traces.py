"""Send demo traces through Alloy's OTLP/HTTP receiver."""

import json
import os
import secrets
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


ENDPOINT = os.environ.get("OTLP_ENDPOINT", "http://alloy:4318/v1/traces")
INTERVAL_SECONDS = 5


def make_trace(trace_id):
    start = time.time_ns()
    return {
        "resourceSpans": [
            {
                "resource": {
                    "attributes": [
                        {"key": "service.name", "value": {"stringValue": "protected-demo"}}
                    ]
                },
                "scopeSpans": [
                    {
                        "scope": {"name": "protected-demo-generator"},
                        "spans": [
                            {
                                "traceId": trace_id,
                                "spanId": secrets.token_hex(8),
                                "name": "protected-demo-request",
                                "startTimeUnixNano": str(start),
                                "endTimeUnixNano": str(start + 1_000_000),
                                "attributes": [
                                    {"key": "secret", "value": {"stringValue": secrets.token_urlsafe(18)}},
                                    {"key": "api.token", "value": {"stringValue": secrets.token_hex(16)}},
                                    {
                                        "key": "customer.email",
                                        "value": {"stringValue": f"user-{secrets.token_hex(6)}@example.invalid"},
                                    },
                                    {"key": "demo.trace_id", "value": {"stringValue": trace_id}},
                                ],
                            }
                        ],
                    }
                ],
            }
        ]
    }


def main():
    while True:
        trace_id = secrets.token_hex(16)
        request = Request(
            ENDPOINT,
            data=json.dumps(make_trace(trace_id)).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urlopen(request, timeout=5) as response:
                status = response.status
        except HTTPError as error:
            status = error.code
        except (URLError, TimeoutError):
            status = "unavailable"
        print(f"trace_id={trace_id} status={status}", flush=True)
        time.sleep(INTERVAL_SECONDS)


if __name__ == "__main__":
    main()

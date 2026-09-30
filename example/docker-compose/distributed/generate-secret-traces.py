"""Send one fictional vault access trace through Alloy's OTLP/HTTP receiver."""

import json
import os
import secrets
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


ENDPOINT = os.environ.get("OTLP_ENDPOINT", "http://alloy:4318/v1/traces")
INTERVAL_SECONDS = 5
AGENTS = {"007": "Julian Bend", "008": "Mara Vale", "009": "Idris Cole"}


def string_attribute(key, value):
    return {"key": key, "value": {"stringValue": value}}


def make_trace(trace_id):
    # One access request, including the responding security staff and hatch operator.
    start = time.time_ns() - 75_000_000_000
    seconds = 1_000_000_000
    timeline = [
        ("vault.access", None, 0, 74, "007", {"vault.zone": "obsidian", "access.status": "released"}),
        ("terminal.session", "vault.access", 3, 28, "007", {"api.token": secrets.token_hex(16)}),
        ("firewall.policy", "terminal.session", 9, 19, "007", {"policy.result": "deny"}),
        ("lockdown.control", "terminal.session", 20, 26, "008", {"alarm.source": "terminal access"}),
        ("security.response", "vault.access", 29, 55, "008", {"response.status": "active"}),
        ("badge.verify", "security.response", 32, 43, "008", {"customer.email": f"badge-{secrets.token_hex(5)}@example.invalid"}),
        ("camera.sweep", "security.response", 37, 47, "008", {"camera.zone": "rack aisle"}),
        ("hatch.route.lookup", "security.response", 48, 54, "009", {"secret": "maintenance hatch B behind rack seven"}),
        ("hatch.exit", "vault.access", 56, 72, "007", {"door.result": "open"}),
        ("door.release", "hatch.exit", 60, 66, "009", {"door.result": "released"}),
    ]
    span_ids = {name: secrets.token_hex(8) for name, *_ in timeline}
    spans = []
    for name, parent, begin, end, agent_number, fields in timeline:
        span = {
            "traceId": trace_id,
            "spanId": span_ids[name],
            "name": name,
            "startTimeUnixNano": str(start + begin * seconds),
            "endTimeUnixNano": str(start + end * seconds),
            "attributes": [
                string_attribute("agent.name", AGENTS[agent_number]),
                string_attribute("agent.number", agent_number),
                *(string_attribute(key, value) for key, value in fields.items()),
            ],
        }
        if parent is not None:
            span["parentSpanId"] = span_ids[parent]
        spans.append(span)
    return {
        "resourceSpans": [{
            "resource": {"attributes": [string_attribute("service.name", "protected-demo")]},
            "scopeSpans": [{"scope": {"name": "protected-demo-generator"}, "spans": spans}],
        }]
    }


def send_trace(payload, endpoint=ENDPOINT):
    request = Request(
        endpoint,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urlopen(request, timeout=5) as response:
        return response.status


def main():
    trace_id = secrets.token_hex(16)
    payload = make_trace(trace_id)
    while True:
        try:
            status = send_trace(payload)
        except HTTPError as error:
            status = error.code
        except (URLError, TimeoutError):
            status = "unavailable"
        print(f"trace_id={trace_id} status={status}", flush=True)
        if isinstance(status, int) and 200 <= status < 300:
            return
        time.sleep(INTERVAL_SECONDS)


if __name__ == "__main__":
    main()

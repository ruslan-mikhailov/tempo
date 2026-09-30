"""Send fictional vault access audit logs through the demo Alloy protector.

The internal source-to-Alloy hop is plaintext; never use real credentials here.
"""

import json
import os
import secrets
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


ENDPOINT = os.environ.get("LOKI_PUSH_URL", "http://alloy-logs:3500/loki/api/v1/push")
INTERVAL_SECONDS = 5
AGENTS = {"007": "Julian Bend", "008": "Mara Vale", "009": "Idris Cole"}
EVENTS = (
    ("007", "access_granted", "Badge authenticated at lower access reader", "accepted"),
    ("007", "terminal_session", "Maintenance terminal session opened", "accepted"),
    ("007", "policy_denied", "Unauthorized export blocked by perimeter policy", "denied"),
    ("008", "lockdown_armed", "East corridor doors sealed and response unit notified", "alert"),
    ("008", "camera_sweep", "Perimeter camera sweep started at rack aisle", "active"),
    ("009", "route_lookup", "Maintenance egress directory queried", "accepted"),
    ("009", "hatch_unlocked", "Rack-seven hatch actuator released", "accepted"),
    ("007", "exit_observed", "Lower service exit sensor closed", "closed"),
)
ENTRY_COUNT = int(os.environ.get("SECRET_LOG_ENTRIES", str(len(EVENTS))))
CUSTOMERS = ("alice@example.invalid", "bob@example.invalid")

def make_push(sequence):
    agent_number, event, message, outcome = EVENTS[sequence % len(EVENTS)]
    agent_name = AGENTS[agent_number]
    email = CUSTOMERS[sequence % len(CUSTOMERS)]
    line = (
        f"event={event} agent_name={json.dumps(agent_name)} agent_number={agent_number} "
        f"message={json.dumps(message)} email={email} "
        f"api_token=demo-{secrets.token_hex(8)} outcome={outcome}"
    )
    return {
        "streams": [{
            "stream": {
                "job": "secret-logs-demo",
                "namespace": "prod",
                "secret_agent": agent_name,
                "secret_agent_number": agent_number,
            },
            "values": [[str(time.time_ns()), line]],
        }]
    }


def send_push(payload, endpoint=ENDPOINT):
    request = Request(
        endpoint,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urlopen(request, timeout=5) as response:
        return response.status


def main():
    sent = 0
    while sent < ENTRY_COUNT:
        try:
            status = send_push(make_push(sent))
        except HTTPError as error:
            status = error.code
        except (URLError, TimeoutError):
            status = "unavailable"
        if status == 204:
            sent += 1
        print(f"loki_push_status={status}", flush=True)
        if sent < ENTRY_COUNT:
            time.sleep(INTERVAL_SECONDS)


if __name__ == "__main__":
    main()

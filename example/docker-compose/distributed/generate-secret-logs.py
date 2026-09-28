"""Send fictional log fields to the demo Alloy protector, not directly to Loki.

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
CUSTOMERS = ("alice@example.invalid", "bob@example.invalid")


def make_push(sequence):
    email = CUSTOMERS[sequence % len(CUSTOMERS)]
    line = f"event=login email={email} api_token=demo-{secrets.token_hex(8)} outcome=accepted"
    return {
        "streams": [
            {
                "stream": {"job": "secret-logs-demo", "namespace": "prod"},
                "values": [[str(time.time_ns()), line]],
            }
        ]
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
    sequence = 0
    while True:
        try:
            status = send_push(make_push(sequence))
        except HTTPError as error:
            status = error.code
        except (URLError, TimeoutError):
            status = "unavailable"
        print(f"loki_push_status={status}", flush=True)
        sequence += 1
        time.sleep(INTERVAL_SECONDS)


if __name__ == "__main__":
    main()

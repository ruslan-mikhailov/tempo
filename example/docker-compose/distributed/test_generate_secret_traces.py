"""Check the exported mission trace and one-shot delivery contract."""

import importlib.util
from pathlib import Path
from unittest import TestCase, mock
from urllib.error import URLError


SCRIPT = Path(__file__).with_name("generate-secret-traces.py")
SPEC = importlib.util.spec_from_file_location("generate_secret_traces", SCRIPT)
producer = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(producer)


class SecretTraceProducerTest(TestCase):
    def test_mission_is_one_trace_with_nested_spans_and_protected_clues(self):
        trace_id = "a" * 32
        payload = producer.make_trace(trace_id)
        self.assertEqual(len(payload["resourceSpans"]), 1)
        spans = payload["resourceSpans"][0]["scopeSpans"][0]["spans"]
        self.assertGreaterEqual(len(spans), 8)
        self.assertEqual({span["traceId"] for span in spans}, {trace_id})
        by_name = {span["name"]: span for span in spans}
        self.assertEqual(len(by_name), len(spans))
        by_id = {span["spanId"]: span for span in spans}
        roots = [span for span in spans if "parentSpanId" not in span]
        self.assertEqual(len(roots), 1)

        identities = set()
        protected_fields = set()
        for span in spans:
            fields = {item["key"]: item["value"]["stringValue"] for item in span["attributes"]}
            self.assertIn("agent.name", fields)
            self.assertRegex(fields["agent.number"], r"^00[7-9]$")
            identities.add((fields["agent.name"], fields["agent.number"]))
            protected_fields.update(fields.keys() & {"secret", "api.token", "customer.email"})
            self.assertNotIn("scene", fields)
            self.assertLess(int(span["startTimeUnixNano"]), int(span["endTimeUnixNano"]))
            for key, value in fields.items():
                if key not in {"agent.name", "agent.number", "secret", "api.token", "customer.email"}:
                    self.assertNotIn(fields["agent.name"], value)
                    self.assertNotIn(fields["agent.number"], value)
            if "parentSpanId" in span:
                parent = by_id[span["parentSpanId"]]
                self.assertLessEqual(int(parent["startTimeUnixNano"]), int(span["startTimeUnixNano"]))
                self.assertLessEqual(int(span["endTimeUnixNano"]), int(parent["endTimeUnixNano"]))
        self.assertIn("008", {number for _, number in identities})
        self.assertGreaterEqual(len(identities), 2)
        self.assertEqual(protected_fields, {"secret", "api.token", "customer.email"})

        self.assertTrue(any(
            "parentSpanId" in by_id[span["parentSpanId"]]
            for span in spans if "parentSpanId" in span
        ))

    def test_transient_failures_retry_the_same_trace_then_exit(self):
        with (
            mock.patch.object(producer, "send_trace", side_effect=[URLError("not ready"), 503, 200, AssertionError("sent a second mission")]) as send,
            mock.patch.object(producer.time, "sleep") as sleep,
        ):
            producer.main()

        self.assertEqual(send.call_count, 3)
        payloads = [call.args[0] for call in send.call_args_list]
        self.assertIs(payloads[0], payloads[1])
        self.assertIs(payloads[1], payloads[2])
        self.assertEqual(sleep.call_count, 2)

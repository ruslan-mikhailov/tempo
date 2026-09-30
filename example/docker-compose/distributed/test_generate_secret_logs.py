"""Keep the disposable log source finite so deletion is not followed by new entries."""

import importlib.util
from pathlib import Path
import shlex
from unittest import TestCase, mock


class SecretLogProducerTest(TestCase):
    def test_configured_batch_exits_without_writing_after_last_entry(self):
        script = Path(__file__).with_name("generate-secret-logs.py")
        spec = importlib.util.spec_from_file_location("generate_secret_logs", script)
        self.assertIsNotNone(spec)
        producer = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(producer)

        with (
            mock.patch.object(producer, "ENTRY_COUNT", 2, create=True),
            mock.patch.object(producer, "send_push", side_effect=[204, 204, AssertionError("source kept writing")]) as send,
            mock.patch.object(producer.time, "sleep") as sleep,
        ):
            producer.main()

        self.assertEqual(send.call_count, 2)
        sleep.assert_called_once_with(producer.INTERVAL_SECONDS)

    def test_transient_push_failure_does_not_consume_the_demo_batch(self):
        script = Path(__file__).with_name("generate-secret-logs.py")
        spec = importlib.util.spec_from_file_location("generate_secret_logs", script)
        producer = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(producer)

        with (
            mock.patch.object(producer, "ENTRY_COUNT", 2),
            mock.patch.object(producer, "send_push", side_effect=[503, 204, 204, AssertionError("source kept writing")]) as send,
            mock.patch.object(producer.time, "sleep"),
        ):
            producer.main()

        self.assertEqual(send.call_count, 3)

    def test_audit_entries_protect_multiple_agent_identities_without_public_copies(self):
        script = Path(__file__).with_name("generate-secret-logs.py")
        spec = importlib.util.spec_from_file_location("generate_secret_logs", script)
        producer = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(producer)

        identities = set()
        entries = []
        for sequence in range(producer.ENTRY_COUNT):
            stream = producer.make_push(sequence)["streams"][0]
            labels = stream["stream"]
            fields = shlex.split(stream["values"][0][1])
            parsed = dict(field.split("=", 1) for field in fields)
            self.assertEqual(len(fields), len(parsed))
            identity = (parsed["agent_name"], parsed["agent_number"])
            identities.add(identity)
            self.assertEqual((labels["secret_agent"], labels["secret_agent_number"]), identity)
            self.assertRegex(identity[1], r"^00[7-9]$")
            self.assertNotIn("scene", parsed)
            self.assertIn("@example.invalid", parsed["email"])
            self.assertTrue(parsed["api_token"].startswith("demo-"))
            entries.append((labels, parsed))

        self.assertIn("008", {number for _, number in identities})
        self.assertGreaterEqual(len(identities), 2)
        for labels, fields in entries:
            public = [*(
                value for key, value in labels.items()
                if key not in {"secret_agent", "secret_agent_number"}
            ), *(
                value for key, value in fields.items()
                if key not in {"agent_name", "agent_number", "email", "api_token"}
            )]
            for name, number in identities:
                for value in public:
                    self.assertNotIn(name, value)
                    self.assertNotIn(number, value)

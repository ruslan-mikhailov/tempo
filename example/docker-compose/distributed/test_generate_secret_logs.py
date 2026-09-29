"""Keep the disposable log source finite so deletion is not followed by new entries."""

import importlib.util
from pathlib import Path
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

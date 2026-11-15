import json
import unittest
from unittest.mock import patch

from dataflow_sdk import EventClient


class EventClientTest(unittest.TestCase):
    @patch("dataflow_sdk.client.urlopen")
    def test_capture_exception_sends_event_and_returns_receipt(self, urlopen):
        response = urlopen.return_value.__enter__.return_value
        response.read.return_value = b'{"duplicate":false}'
        client = EventClient("http://localhost:8080/", "checkout")

        receipt = client.capture_exception(ValueError("order 42 failed"))

        request = urlopen.call_args.args[0]
        event = json.loads(request.data.decode("utf-8"))
        self.assertEqual("http://localhost:8080/api/events", request.full_url)
        self.assertEqual("POST", request.get_method())
        self.assertEqual("application/json", request.get_header("Content-type"))
        self.assertEqual("checkout", event["project"])
        self.assertEqual("ValueError", event["exceptionType"])
        self.assertEqual("order 42 failed", event["message"])
        self.assertIn("ValueError: order 42 failed", event["stackTrace"])
        self.assertEqual({"duplicate": False}, receipt)

    def test_requires_project_and_positive_timeout(self):
        with self.assertRaises(ValueError):
            EventClient("http://localhost:8080", " ")
        with self.assertRaises(ValueError):
            EventClient("http://localhost:8080", "checkout", timeout=0)

    @patch("dataflow_sdk.client.urlopen", side_effect=TimeoutError("request timed out"))
    def test_network_errors_are_visible_to_the_caller(self, _urlopen):
        client = EventClient("http://localhost:8080", "checkout")

        with self.assertRaisesRegex(TimeoutError, "request timed out"):
            client.capture_exception(RuntimeError("failure"))


if __name__ == "__main__":
    unittest.main()

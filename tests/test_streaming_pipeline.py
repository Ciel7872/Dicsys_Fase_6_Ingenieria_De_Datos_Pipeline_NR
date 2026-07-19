import sys
import json
import unittest
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from dataflow.streaming_pipeline import ParseEvent


def _ignore_ingestion_time(expected_fields):
    def check(actual):
        actual = list(actual)
        assert len(actual) == 1, f"Expected 1 result, got {len(actual)}"
        row = actual[0]
        for k, v in expected_fields.items():
            assert row[k] == v, f"{k}: expected {v}, got {row[k]}"
        assert "ingestion_time" in row, "missing ingestion_time"
    return check


class ParseEventTests(unittest.TestCase):
    def test_parse_valid_event(self):
        event = json.dumps({
            "event_id": 1001,
            "event_type": "login",
            "customer_id": 101,
            "product_id": 0,
            "session_id": "sess_abc123",
            "event_timestamp": "2026-07-18T10:00:00Z",
        }).encode("utf-8")

        with TestPipeline() as p:
            result = p | beam.Create([event]) | beam.ParDo(ParseEvent())
            assert_that(result, _ignore_ingestion_time({
                "event_id": 1001,
                "date_id": 20260718,
                "customer_id": 101,
                "product_id": 0,
                "session_id": "sess_abc123",
                "event_type": "login",
            }))

    def test_parse_invalid_event_goes_to_deadletter(self):
        invalid = b"not json"

        with TestPipeline() as p:
            result = p | beam.Create([invalid]) | beam.ParDo(ParseEvent()).with_outputs("deadletter", main="success")
            deadletter = result["deadletter"]
            def check_deadletter(actual):
                actual = list(actual)
                assert len(actual) == 1, f"Expected 1 deadletter, got {len(actual)}"
                row = actual[0]
                assert "raw_message" in row
                assert "error" in row
                assert "ingestion_time" in row
            assert_that(deadletter, check_deadletter)

    def test_parse_event_truncates_long_fields(self):
        event = json.dumps({
            "event_id": 1001,
            "event_type": "x" * 100,
            "customer_id": 101,
            "product_id": 0,
            "session_id": "s" * 300,
            "event_timestamp": "2026-07-18T10:00:00Z",
        }).encode("utf-8")

        with TestPipeline() as p:
            result = p | beam.Create([event]) | beam.ParDo(ParseEvent())
            assert_that(result, _ignore_ingestion_time({
                "event_id": 1001,
                "date_id": 20260718,
                "customer_id": 101,
                "product_id": 0,
                "session_id": "s" * 255,
                "event_type": "x" * 50,
            }))


if __name__ == "__main__":
    unittest.main()

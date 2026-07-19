import sys
import json
import unittest
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))


class GenerateEventsTests(unittest.TestCase):
    def setUp(self):
        self.project_root = Path(__file__).resolve().parents[1]
        self.generator_path = self.project_root / "data" / "generate" / "generate_events.py"

    def test_generator_script_exists(self):
        self.assertTrue(self.generator_path.exists(), "generate_events.py not found")

    def test_generator_output_exists(self):
        output_path = self.project_root / "data" / "eventos_ing.json"
        self.assertTrue(output_path.exists(), "eventos_ing.json not found")

    def test_generated_json_is_valid(self):
        output_path = self.project_root / "data" / "eventos_ing.json"
        with open(output_path, "r") as f:
            data = json.load(f)
        self.assertIsInstance(data, list, "JSON root should be a list")
        self.assertGreater(len(data), 0, "JSON should not be empty")

    def test_generated_events_have_required_fields(self):
        output_path = self.project_root / "data" / "eventos_ing.json"
        with open(output_path, "r") as f:
            data = json.load(f)
        required_fields = ["event_id", "event_type", "customer_id", "product_id", "session_id", "event_timestamp"]
        for event in data[:5]:
            for field in required_fields:
                self.assertIn(field, event, f"Field '{field}' missing in event")

    def test_generated_events_have_valid_event_types(self):
        valid_types = {"login", "view", "add_to_cart", "checkout", "purchase", "cart_abandoned"}
        output_path = self.project_root / "data" / "eventos_ing.json"
        with open(output_path, "r") as f:
            data = json.load(f)
        for event in data:
            self.assertIn(event["event_type"], valid_types, f"Invalid event_type: {event['event_type']}")


if __name__ == "__main__":
    unittest.main()

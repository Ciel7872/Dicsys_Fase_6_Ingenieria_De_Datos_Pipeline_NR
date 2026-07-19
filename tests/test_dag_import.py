import sys
import unittest
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))


class DagImportTests(unittest.TestCase):
    def test_dag_file_exists(self):
        dag_path = Path(__file__).resolve().parents[1] / "dags" / "dag_eventos_realtime.py"
        self.assertTrue(dag_path.exists(), f"DAG file not found: {dag_path}")

    def test_dag_file_has_required_tasks(self):
        dag_path = Path(__file__).resolve().parents[1] / "dags" / "dag_eventos_realtime.py"
        content = dag_path.read_text(encoding="utf-8")
        required_tasks = [
            "setup_infrastructure",
            "launch_dataflow_job",
            "dbt_run",
            "validate_data",
        ]
        for task in required_tasks:
            self.assertIn(task, content, f"Task '{task}' not found in DAG")

    def test_dag_has_correct_schedule(self):
        dag_path = Path(__file__).resolve().parents[1] / "dags" / "dag_eventos_realtime.py"
        content = dag_path.read_text(encoding="utf-8")
        self.assertIn("*/10 * * * *", content, "Schedule interval not found")

    def test_dag_has_dbt_command(self):
        dag_path = Path(__file__).resolve().parents[1] / "dags" / "dag_eventos_realtime.py"
        content = dag_path.read_text(encoding="utf-8")
        self.assertIn("dbt run", content, "dbt run command not found")


if __name__ == "__main__":
    unittest.main()

import sys
import unittest
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from dags.dependencies.task_factory import (
    build_bq_table_ref,
    build_dataflow_parameters,
    load_sql_query,
    resolve_project_root,
)


class TaskFactoryTests(unittest.TestCase):
    def test_build_bq_table_ref(self):
        self.assertEqual(
            build_bq_table_ref("proj", "ds", "table"),
            "proj:ds.table",
        )

    def test_build_dataflow_parameters(self):
        params = build_dataflow_parameters("proj", "topic", "out", "dead")
        self.assertEqual(params["project"], "proj")
        self.assertEqual(params["topic"], "topic")
        self.assertEqual(params["output-table"], "out")
        self.assertEqual(params["deadletter-table"], "dead")

    def test_load_sql_query(self):
        sql_path = resolve_project_root() / "sql" / "transforms" / "transform_bronze_to_curated.sql"
        sql = load_sql_query(sql_path)
        self.assertIn("FACT_EVENTS", sql)
        self.assertIn("bronze_events", sql)


if __name__ == "__main__":
    unittest.main()

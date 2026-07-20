import importlib.util
from pathlib import Path
import unittest


MODULE_PATH = Path(__file__).parents[1] / "bootstrap.py"
SPEC = importlib.util.spec_from_file_location("broker_postgres_bootstrap", MODULE_PATH)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError(f"unable to load {MODULE_PATH}")
bootstrap = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(bootstrap)


class BootstrapHelpersTests(unittest.TestCase):
    def test_project_name_normalizes_legacy_broker_values(self):
        self.assertEqual(bootstrap.project_name("riperis"), "ripe-ris")
        self.assertEqual(bootstrap.project_name("routeviews"), "route-views")

    def test_project_name_rejects_unknown_values(self):
        with self.assertRaises(ValueError):
            bootstrap.project_name("other")

    def test_copy_row_preserves_zero_sizes_from_sqlite(self):
        self.assertEqual(
            bootstrap.copy_row((1704067200, 7, 2, 0, 1234), {7: 11}, {2: "rib"}),
            (1704067200, 11, "rib", 0, 1234),
        )

    def test_copy_row_rejects_unmapped_collector_or_type(self):
        with self.assertRaises(ValueError):
            bootstrap.copy_row((1704067200, 7, 2, 0, 1234), {}, {2: "rib"})
        with self.assertRaises(ValueError):
            bootstrap.copy_row((1704067200, 7, 2, 0, 1234), {7: 11}, {})

    def test_schema_uses_operational_update_metadata_without_provenance_tables(self):
        schema = bootstrap.schema_sql()
        self.assertIn("CREATE TABLE IF NOT EXISTS mrt.update_meta", schema)
        self.assertNotIn("meta.source_revision", schema)
        self.assertNotIn("meta.ingest_run", schema)


if __name__ == "__main__":
    unittest.main()

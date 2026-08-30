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
        self.assertIn("CREATE TABLE IF NOT EXISTS broker.update_meta", schema)
        self.assertNotIn("meta.source_revision", schema)
        self.assertNotIn("meta.ingest_run", schema)

    def test_schema_uses_broker_namespace_and_suffixes_read_views(self):
        schema = bootstrap.schema_sql()
        self.assertIn("CREATE SCHEMA IF NOT EXISTS broker", schema)
        self.assertIn("CREATE TABLE IF NOT EXISTS broker.file", schema)
        self.assertIn("CREATE OR REPLACE VIEW broker.file_search_view", schema)
        self.assertIn("CREATE OR REPLACE VIEW broker.file_latest_view", schema)
        self.assertNotIn("mrt.", schema)
        self.assertNotIn("api.", schema)

    def test_live_migration_renames_broker_schema_and_views_without_dropping_data(self):
        migration = (Path(__file__).parents[1] / "migrate_mrt_api_to_broker.sql").read_text()
        self.assertIn("ALTER SCHEMA mrt RENAME TO broker", migration)
        self.assertIn("ALTER VIEW api.mrt_file_search SET SCHEMA broker", migration)
        self.assertIn("ALTER VIEW broker.mrt_file_search RENAME TO file_search_view", migration)
        self.assertIn("ALTER VIEW api.mrt_file_latest SET SCHEMA broker", migration)
        self.assertIn("ALTER VIEW broker.mrt_file_latest RENAME TO file_latest_view", migration)
        self.assertNotIn("DROP SCHEMA IF EXISTS mrt CASCADE", migration)

    def test_reset_checks_for_inbound_foreign_keys_before_dropping_broker(self):
        sql = bootstrap.reset_dependency_check_sql()
        self.assertIn("constraint_type = 'FOREIGN KEY'", sql)
        self.assertIn("table_schema = 'broker'", sql)
        self.assertIn("table_schema <> 'broker'", sql)

    def test_reset_uses_restrictive_schema_drop(self):
        source = MODULE_PATH.read_text()
        self.assertNotIn("DROP SCHEMA IF EXISTS broker CASCADE", source)
        self.assertEqual(
            bootstrap.reset_drop_statements(),
            (
                "DROP VIEW IF EXISTS broker.file_search_view",
                "DROP VIEW IF EXISTS broker.file_latest_view",
                "DROP TABLE IF EXISTS broker.update_meta",
                "DROP TABLE IF EXISTS broker.latest_file",
                "DROP TABLE IF EXISTS broker.file",
                "DROP TABLE IF EXISTS broker.collector",
                "DROP TABLE IF EXISTS broker.project",
                "DROP SCHEMA IF EXISTS broker",
            ),
        )


if __name__ == "__main__":
    unittest.main()

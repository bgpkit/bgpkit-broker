-- Consolidate the legacy PostgreSQL Broker namespaces into broker.
--
-- Preconditions:
--   * Stop Broker and Spectrum writers.
--   * Take a verified PostgreSQL backup.
--   * Run as the owner of mrt and api, or a PostgreSQL superuser.
--
-- This is a metadata-only migration. ALTER SCHEMA preserves table rows, indexes,
-- constraints, sequences, and foreign-key dependencies, including Spectrum's
-- foreign key to the canonical Broker file identity.

BEGIN;

LOCK TABLE
    mrt.project,
    mrt.collector,
    mrt.file,
    mrt.latest_file,
    mrt.update_meta
IN ACCESS EXCLUSIVE MODE;

ALTER SCHEMA mrt RENAME TO broker;

ALTER INDEX IF EXISTS broker.mrt_file_collector_type_ts_idx
    RENAME TO broker_file_collector_type_ts_idx;
ALTER INDEX IF EXISTS broker.mrt_file_type_ts_idx
    RENAME TO broker_file_type_ts_idx;
ALTER INDEX IF EXISTS broker.mrt_update_meta_ts_idx
    RENAME TO broker_update_meta_ts_idx;

ALTER VIEW api.mrt_file_search SET SCHEMA broker;
ALTER VIEW broker.mrt_file_search RENAME TO file_search_view;
ALTER VIEW api.mrt_file_latest SET SCHEMA broker;
ALTER VIEW broker.mrt_file_latest RENAME TO file_latest_view;

-- The two views were the only intended api objects. This fails rather than
-- cascading if an unexpected object remains, preserving an audit opportunity.
DROP SCHEMA api;

COMMIT;

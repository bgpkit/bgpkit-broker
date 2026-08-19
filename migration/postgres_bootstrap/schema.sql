-- PostgreSQL catalog backing the BGPKIT Broker API.
-- Raw MRT payloads remain in archive/object storage; this database indexes metadata.

CREATE SCHEMA IF NOT EXISTS broker;

CREATE TABLE IF NOT EXISTS broker.project (
    project_id SMALLINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name TEXT NOT NULL UNIQUE CHECK (name IN ('ripe-ris', 'route-views'))
);

CREATE TABLE IF NOT EXISTS broker.collector (
    collector_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    project_id SMALLINT NOT NULL REFERENCES broker.project(project_id),
    name TEXT NOT NULL,
    base_uri TEXT NOT NULL,
    updates_interval_seconds INTEGER NOT NULL CHECK (updates_interval_seconds > 0),
    UNIQUE (project_id, name)
);

-- One row is an observed archive object catalog entry, not a guarantee of reachability.
CREATE TABLE IF NOT EXISTS broker.file (
    ts_start TIMESTAMPTZ NOT NULL,
    collector_id BIGINT NOT NULL REFERENCES broker.collector(collector_id),
    data_type TEXT NOT NULL CHECK (data_type IN ('rib', 'updates')),
    rough_size BIGINT NOT NULL DEFAULT 0 CHECK (rough_size >= 0),
    exact_size BIGINT NOT NULL DEFAULT 0 CHECK (exact_size >= 0),
    PRIMARY KEY (ts_start, collector_id, data_type)
);

-- Compatibility/read model: refreshed after each complete ingest.
CREATE TABLE IF NOT EXISTS broker.latest_file (
    collector_id BIGINT NOT NULL REFERENCES broker.collector(collector_id),
    data_type TEXT NOT NULL CHECK (data_type IN ('rib', 'updates')),
    ts_start TIMESTAMPTZ NOT NULL,
    rough_size BIGINT NOT NULL DEFAULT 0,
    exact_size BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (collector_id, data_type)
);

-- Mirrors the SQLite Broker `meta` table for operational update status.
CREATE TABLE IF NOT EXISTS broker.update_meta (
    update_ts TIMESTAMPTZ NOT NULL,
    update_duration_seconds INTEGER NOT NULL,
    insert_count INTEGER NOT NULL
);

CREATE OR REPLACE VIEW broker.file_search_view AS
SELECT
    f.ts_start AS timestamp,
    f.rough_size,
    f.exact_size,
    f.data_type AS type,
    c.name AS collector_name,
    c.base_uri AS collector_url,
    p.name AS project_name,
    c.updates_interval_seconds AS updates_interval
FROM broker.file AS f
JOIN broker.collector AS c USING (collector_id)
JOIN broker.project AS p USING (project_id);

CREATE OR REPLACE VIEW broker.file_latest_view AS
SELECT
    l.ts_start AS timestamp,
    l.rough_size,
    l.exact_size,
    l.data_type AS type,
    c.name AS collector_name,
    c.base_uri AS collector_url,
    p.name AS project_name,
    c.updates_interval_seconds AS updates_interval
FROM broker.latest_file AS l
JOIN broker.collector AS c USING (collector_id)
JOIN broker.project AS p USING (project_id);

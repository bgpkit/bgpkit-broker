# Production Database Cleanup Plan for Issue #97 (Collector-Specific)

## Problem Summary
The production database contains erroneous entries with Unix epoch timestamp (1970-01-01 00:00:00, timestamp=0). These entries appear in search results when users don't provide time filtering.

## Collector Activation Dates (from bgpkit-mcp)

| Collector | Project | Activated On |
|-----------|---------|--------------|
| rrc00 | riperis | 1999-10-01 |
| rrc01 | riperis | 2000-07-01 |
| route-views2 | routeviews | 2001-10-30 |

All other collectors have later activation dates (1999 or later).

## SQL Queries for Targeted Cleanup

### 1. Check for Erroneous Entries by Collector

```sql
-- Find entries before each collector's activation date
SELECT 
    c.name as collector_name,
    c.project,
    MIN(datetime(f.timestamp, 'unixepoch')) as earliest_file_ts,
    COUNT(*) as invalid_count
FROM files f
JOIN collectors c ON f.collector_id = c.id
WHERE 
    (c.name = 'rrc00' AND f.timestamp < strftime('%s', '1999-10-01')) OR
    (c.name = 'rrc01' AND f.timestamp < strftime('%s', '2000-07-01')) OR
    (c.name = 'route-views2' AND f.timestamp < strftime('%s', '2001-10-30')) OR
    (c.name NOT IN ('rrc00', 'rrc01', 'route-views2') AND f.timestamp < strftime('%s', '1999-10-01'))
GROUP BY c.name, c.project
ORDER BY invalid_count DESC;
```

### 2. Detailed View of Erroneous Entries

```sql
-- View actual erroneous entries with collector context
SELECT 
    f.timestamp,
    datetime(f.timestamp, 'unixepoch') as ts_readable,
    c.name as collector_name,
    c.project,
    t.name as type_name,
    f.rough_size,
    f.exact_size,
    CASE c.name
        WHEN 'rrc00' THEN '1999-10-01'
        WHEN 'rrc01' THEN '2000-07-01'
        WHEN 'route-views2' THEN '2001-10-30'
        ELSE '1999-10-01'
    END as collector_activated
FROM files f
JOIN collectors c ON f.collector_id = c.id
JOIN types t ON f.type_id = t.id
WHERE 
    (c.name = 'rrc00' AND f.timestamp < strftime('%s', '1999-10-01')) OR
    (c.name = 'rrc01' AND f.timestamp < strftime('%s', '2000-07-01')) OR
    (c.name = 'route-views2' AND f.timestamp < strftime('%s', '2001-10-30')) OR
    (c.name NOT IN ('rrc00', 'rrc01', 'route-views2') AND f.timestamp < strftime('%s', '1999-10-01'))
ORDER BY c.name, f.timestamp;
```

### 3. Delete Erroneous Entries (Collector-Specific)

**WARNING: This permanently deletes data. Run the SELECT queries above first to verify!**

```sql
-- Delete entries before each collector's activation date
DELETE FROM files 
WHERE id IN (
    SELECT f.id
    FROM files f
    JOIN collectors c ON f.collector_id = c.id
    WHERE 
        (c.name = 'rrc00' AND f.timestamp < strftime('%s', '1999-10-01')) OR
        (c.name = 'rrc01' AND f.timestamp < strftime('%s', '2000-07-01')) OR
        (c.name = 'route-views2' AND f.timestamp < strftime('%s', '2001-10-30')) OR
        (c.name NOT IN ('rrc00', 'rrc01', 'route-views2') AND f.timestamp < strftime('%s', '1999-10-01'))
);
```

Alternative: If you only want to delete exact timestamp 0 (most conservative):
```sql
-- Delete only entries with timestamp exactly 0 (1970-01-01)
DELETE FROM files 
WHERE timestamp = 0;
```

### 4. Verify Cleanup

```sql
-- Verify no more erroneous entries exist
SELECT 
    c.name as collector_name,
    COUNT(*) as remaining_invalid
FROM files f
JOIN collectors c ON f.collector_id = c.id
WHERE 
    (c.name = 'rrc00' AND f.timestamp < strftime('%s', '1999-10-01')) OR
    (c.name = 'rrc01' AND f.timestamp < strftime('%s', '2000-07-01')) OR
    (c.name = 'route-views2' AND f.timestamp < strftime('%s', '2001-10-30')) OR
    (c.name NOT IN ('rrc00', 'rrc01', 'route-views2') AND f.timestamp < strftime('%s', '1999-10-01'))
GROUP BY c.name
ORDER BY remaining_invalid DESC;
```

Should return no rows (or all counts = 0).

## Railway Deployment Cleanup Steps

### Option A: Railway CLI (Recommended)

1. **Access the Railway database container**:
   ```bash
   railway login
   railway link
   railway up
   # OR: railway connect
   ```

2. **Check current state**:
   ```bash
   railway run "sqlite3 /data/bgpkit_broker.sqlite3 '
   SELECT c.name, MIN(datetime(f.timestamp, \"unixepoch\")) as earliest, COUNT(*) as count
   FROM files f
   JOIN collectors c ON f.collector_id = c.id
   WHERE f.timestamp < strftime(\"%s\", \"1999-10-01\")
   GROUP BY c.name;'"
   ```

3. **Delete erroneous entries**:
   ```bash
   railway run "sqlite3 /data/bgpkit_broker.sqlite3 '
   DELETE FROM files 
   WHERE id IN (
       SELECT f.id FROM files f
       JOIN collectors c ON f.collector_id = c.id
       WHERE (c.name = \"rrc00\" AND f.timestamp < strftime(\"%s\", \"1999-10-01\")) OR
             (c.name = \"rrc01\" AND f.timestamp < strftime(\"%s\", \"2000-07-01\")) OR
             (c.name = \"route-views2\" AND f.timestamp < strftime(\"%s\", \"2001-10-30\")) OR
             (c.name NOT IN (\"rrc00\", \"rrc01\", \"route-views2\") AND f.timestamp < strftime(\"%s\", \"1999-10-01\"))
   );'"
   ```

4. **Verify deletion**:
   ```bash
   railway run "sqlite3 /data/bgpkit_broker.sqlite3 'SELECT COUNT(*) FROM files WHERE timestamp < strftime(\"%s\", \"1998-01-01\");'"
   # Should output: 0
   ```

### Option B: Simple DELETE (Conservative)

If you want the simplest approach, just delete all entries with timestamp exactly 0:

```bash
railway run "sqlite3 /data/bgpkit_broker.sqlite3 'DELETE FROM files WHERE timestamp = 0;'"
```

This is the safest approach because:
- Timestamp 0 is definitely invalid (1970-01-01)
- No legitimate collector data exists from 1970
- Minimal risk of deleting valid data

## Post-Cleanup Verification

After cleanup, verify via API:

```bash
# Should NOT return 1970 entries anymore
curl "https://api.bgpkit.com/v3/broker/search?collector_id=rrc00,rrc01,route-views2&page_size=10"
```

Check that the first entries have reasonable timestamps:
- rrc00: Should be 1999-10-01 or later
- rrc01: Should be 2000-07-01 or later
- route-views2: Should be 2001-10-30 or later

## Bootstrap Database Update

If you maintain a bootstrap database file:

1. Download the current bootstrap DB locally
2. Run the collector-specific cleanup query
3. Re-upload the cleaned database

```bash
# Local cleanup of bootstrap DB
sqlite3 bgpkit_broker_bootstrap.sqlite3 "DELETE FROM files WHERE timestamp = 0;"

# Or the collector-specific version:
sqlite3 bgpkit_broker_bootstrap.sqlite3 "
DELETE FROM files 
WHERE id IN (
    SELECT f.id FROM files f
    JOIN collectors c ON f.collector_id = c.id
    WHERE (c.name = 'rrc00' AND f.timestamp < strftime('%s', '1999-10-01')) OR
          (c.name = 'rrc01' AND f.timestamp < strftime('%s', '2000-07-01')) OR
          (c.name = 'route-views2' AND f.timestamp < strftime('%s', '2001-10-30')) OR
          (c.name NOT IN ('rrc00', 'rrc01', 'route-views2') AND f.timestamp < strftime('%s', '1999-10-01'))
);"

# Verify
sqlite3 bgpkit_broker_bootstrap.sqlite3 "SELECT COUNT(*) FROM files WHERE timestamp < strftime('%s', '1998-01-01');"
# Should output: 0
```

## Monitoring

After deployment and cleanup:

1. Check API response for 1970 entries:
   ```bash
   curl -s "https://api.bgpkit.com/v3/broker/search?collector_id=rrc00&page_size=1" | jq '.data[0].ts_start'
   ```

2. Watch application logs for warnings:
   ```
   Skipping item with invalid timestamp: 1970-01-01T00:00:00 < 1999-10-01T00:00:00
   ```

## Summary

- **Root cause**: Unknown parsing error resulted in timestamp 0 entries
- **Impact**: 3 entries found (rrc00, rrc01, route-views2) with 1970-01-01 timestamps
- **Code fix**: Validation added at crawler and database layers
- **Cleanup**: Run `DELETE FROM files WHERE timestamp = 0;` on production DB
- **Future prevention**: Crawlers now filter pre-activation date entries

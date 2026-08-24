# Backfill Runbook — edp_backfill_manual

Audience: on-call data platform engineer. Read fully before first trigger.

## When to backfill

| Scenario | Action |
|---|---|
| Silver merge job failed for N hours (gap in history) | Backfill silver only |
| Bronze stream down + Kafka retention about to expire | Backfill bronze FIRST, then silver |
| Bad logic deployed and fixed; need replay | Backfill affected entity end-to-end |
| Upstream re-sent corrected data with same keys | **No backfill needed** — MERGE is idempotent |

## Safety model — why clearing/re-running is safe

1. **Bronze**: Delta idempotent writes (`txnAppId`/`txnVersion`) — a repeated
   availableNow run over the same offsets writes nothing new. Offsets live in
   checkpoints, not in Airflow.
2. **Silver SCD1**: keyed MERGE — replays overwrite identically.
3. **Silver SCD2**: `xxhash64` row-hash change detection — unchanged rows are
   skipped, so replays do NOT fabricate history.
4. **Quarantine**: invalid rows land in `_quarantine/` with reasons — review
   after every backfill (query below).

## Procedure

### 1. Pre-flight
- [ ] Confirm the continuous streams' health (no active incident overlapping
      the tables you'll write).
- [ ] Check Kafka retention covers your gap: messages older than retention are
      GONE — coordinate with producers for source-side replay instead.
- [ ] Note current quarantine row count as baseline.

```bash
# Airflow CLI trigger, restricting to one entity:
airflow dags trigger edp_backfill_manual \
  --conf '{"silver_jobs_json":[{"name":"payments_silver",
    "source_path":"gs://edp-bronze-raw/bronze/payments",
    "target_path":"gs://edp-silver/silver/payments",
    "primary_keys":["payment_id"],"scd_type":2}]}'
```

### 2. Monitor
- Watch task `bronze_backfill` → `silver_backfill`; SLA is 8h.
- Spark UI: look for skewed tasks in the MERGE stage (see study notes:
  salting/AQE) before assuming hang.

### 3. Post-backfill verification
```sql
-- Quarantine delta (what failed validation this run):
SELECT quarantine_reason, COUNT(*) FROM delta.`<source>_quarantine`
GROUP BY 1 ORDER BY 2 DESC;

-- SCD2 sanity: exactly one current row per key:
SELECT key, COUNTIF(_is_current) AS currents, COUNT(*) AS versions
FROM delta.`<target>` GROUP BY key HAVING currents <> 1;

-- Freshness: pipeline_monitor.pipeline_runs latest success per pipeline.
```

- [ ] Row counts plausible vs source topic offsets
- [ ] DQ gate suite passes against backfilled range
- [ ] Quarantine delta within expected bounds vs baseline

### 4. If wrong: stop, don't patch forward
Time travel makes rollback cheap:
```sql
SELECT * FROM (DESCRIBE HISTORY delta.`<target>`) ORDER BY version DESC;
RESTORE TABLE delta.`<target>` TO VERSION AS OF <good_version>;
```
Escalate P2 → platform channel before any RESTORE beyond dev.

## VACUUM warning
Never run real VACUUM (dry-run off) until the backfill is verified — it
destroys the time-travel safety net you may need within retention hours.

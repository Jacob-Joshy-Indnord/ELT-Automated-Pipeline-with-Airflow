# New sales source: contract, validation and release

Validation and deployment use the **existing workflow only**: DAG `online-sales-ingest-dbt-orchestrator`
and the existing Docker Compose path. No separate release pipeline.

## Status: PROVISIONAL
All assumptions live in `request-script/new_source_contract.json` (access, key, timezone UTC, currency USD,
no conversion, missing-field policy, cutoff `06:00` UTC, zero-row = fail, late = >1 day). dbt defaults in
`dbt_project.yml` `vars:` mirror the thresholds; the DAG passes the contract values to dbt as `--vars`.
No vendor/format is assumed; CSV in `data/new_source/<run_date>.csv` is a stand-in. Placeholder values
need business approval (freshness/zero-row thresholds) and source-owner sign-off (currency, key overlap,
missing-field conversions).

## Known limits (open)
- Deletes are unsupported; updates = higher `version` of the same `source_key` wins in staging.
- Legacy replay guard (`staging.legacy_load_log`): if the legacy table already holds rows from earlier
  unguarded runs, seed the log for those dates before enabling, or the first guarded run re-appends once.
- Alert channel is log-only (`pipeline_gate.alert_failure`); wire email/webhook when known.
- Reviewers/approvers and the nonproduction environment are NOT known: record them in
  `approval.sdlc_approvers` / `approval.nonproduction_environment`.

## Promotion checklist (nonproduction first)
1. `python airflow/scripts/docker_preflight.py` (set `DBT_HOST_PROJECT_DIR`, `DBT_HOST_PROFILES_FILE`): docker_url, mounts, nonzero dbt exit.
2. `pytest tests` (loader fixtures, gate, replay guard) and `dbt build` (models + tests) green.
3. Clear and rerun a run date and run a backfill: row counts and report totals unchanged (legacy and new).
4. Simulate a missed cutoff (set Variable `new_source_daily_cutoff_utc` in the past): task fails, no `staging.daily_refresh_log` row, alert logged.
5. Source owner approves the contract; update `new_source_contract.json` (`status`/`approval`), reconcile code and tests, rerun 1-4.
6. `python request-script/release_gate.py` must exit 0; named SDLC approvers sign off the evidence.
7. Promote schema/config/secrets via Compose, smoke-test report freshness (`staging.daily_refresh_log`).

## Operations
- Pause new-source publication: pause the DAG in Airflow (or remove the day's file; the load fails and no success is declared).
- Recovery/replay: fix the file, clear the run date; landing upserts and the legacy guard make replay safe.
- Verify freshness daily: latest `staging.daily_refresh_log.run_date` is yesterday.

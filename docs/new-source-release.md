# New sales source: validation and release (existing workflow)

Runs through the existing DAG `online-sales-ingest-dbt-orchestrator` and the
existing Docker Compose deploy path (README). No separate release pipeline.
Approvers and the non-production environment are not evidenced in the repo —
record them below when known.

- Source owner: ______  Release approver(s): ______  Non-prod environment: ______

## Gate 0 — contract
`docs/new-source-contract.md` signed off and code/tests reconciled. Blocks everything below.

## Gate 1 — non-production validation (attach evidence)
1. `python -m pytest tests` (loader fixtures: valid, duplicate, malformed, missing, empty).
2. `dbt build` against non-prod with fixture batch: all 21+ data tests pass.
3. DAG integration: trigger the DAG; `daily_refresh_complete` succeeds.
4. Replay: clear and rerun `ingest_new_source`; landing/report counts unchanged.
5. Legacy regression: `new_source_report_reconciliation` passes (legacy totals unchanged).
6. Verify before enabling: `DBT_PROJECT_HOST_PATH`, `DBT_PROFILES_HOST_PATH`,
   `DBT_DOCKER_NETWORK` resolve on the target host; `docker_url` works.
   Legacy backfill/rerun caution: the legacy CSV loader still appends, so do not
   clear/rerun a successful `ingest_data` task or backfill runs.

## Gate 2 — approval and promotion
Named approvers sign off on Gate 1 evidence. Promote schema/config/secrets
(env vars above, drop-file location), then `docker-compose up --build`.

## Post-release
- Smoke test: newest `new_source.load_batches.loaded_at` is today;
  `sales_report` contains `source_system='new_source'` rows via `stg_all_sales`.
- Monitor: any failure logs an `ALERT` (task `on_failure_callback`); hook it to
  the team's real alert channel when known.
- Freshness is enforced daily by dbt test `new_source_batch_freshness_and_volume`.

## Pause / rollback / recovery
- Pause publication: set Airflow Variable `new_source_publication_enabled=false`.
  Ingestion is skipped and dbt excludes new-source rows; legacy reports unaffected.
- Resume: set to `true` and trigger the DAG; the next run publishes landed data.
- Replay after a bad batch: fix the source file (new content → new batch id, new
  `source_version` for corrected rows), rerun the DAG. Rejected rows are in
  `new_source.sales_rejects` with batch id and reason.
- Full rollback: pause, then drop/truncate `new_source.*` if the owner requires removal, rerun `dbt run`.

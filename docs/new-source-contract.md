# New sales source: contract (PROVISIONAL — pending source owner approval)

No vendor, protocol or schema is evidenced in the repository. Everything below
is an **assumption used to build and test with synthetic fixtures**
(`tests/fixtures/new_source_sample.csv`). Do not publish real new-source data to
production until the owner approves this contract and code/tests are reconciled.

## Provisional assumptions
| Item | Assumption | Where implemented |
|---|---|---|
| Access/protocol | Daily CSV drop at `data/raw/new_source_sales.csv` (override `NEW_SOURCE_FILE`) | `request-script/new_source_loader.py` |
| Credentials | DB creds via `DB_HOST/DB_PORT/DB_NAME/DB_USER/DB_PASSWORD` env (defaults match legacy); no source credentials needed for file drop | loader `connect()` |
| Columns | `source_key, event_time, quantity, unit_price` required; `source_version, description, country, payment_method, category, sales_channel, return_status, order_priority, discount, shipping_cost` optional | loader |
| Business key | `source_key` + `source_version` (default 1) | landing PK |
| Updates | Higher `source_version` supersedes lower for the same key; staging keeps the latest version | `stg_new_source_sales` |
| Deletes | Not supported (no tombstones) | — |
| Timezone | `event_time` is UTC (offsets normalised to UTC) | loader |
| Currency | Same currency as legacy feed; no conversion | `stg_new_source_sales` |
| Daily cutoff | Ingest task must finish within 6h of schedule (`NEW_SOURCE_CUTOFF_HOURS`) | DAG `execution_timeout` |
| Missing-field policy | Missing/invalid required field → record quarantined in `new_source.sales_rejects`; optional fields become NULL | loader |
| Freshness / volume | Newest batch ≤ 36h old and ≥ 1 accepted row (`new_source_max_lateness_hours`, `new_source_min_accepted_rows`) | dbt test |
| Replay | Batch id = hash of file content; identical replay is a no-op | loader |

## Owner approval (required before production)
- [ ] Access/protocol and accessible sample — Approver: ______ Date: ______
- [ ] Field/type mapping, currency, timezone — ______
- [ ] Stable key, update and deletion rules — ______
- [ ] Daily delivery cutoff, lateness and zero-row thresholds — ______
- [ ] Missing-field / reject policy — ______
- [ ] Implementation and tests reconciled with approved contract (PR/commit: ______)

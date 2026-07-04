# Project Roadmap — Databricks Lakehouse (NYC Taxi / mobility domain)

End-to-end batch/streaming lakehouse on Azure Databricks using the medallion
architecture, Unity Catalog, a CLI-first workflow, and full CI/CD across three
environments.

Status: ✅ done · 🔶 partial · ⬜ pending

## Scope

| Area | Item | Status |
|---|---|---|
| Governance | Unity Catalog: catalogs, schemas, volumes | ✅ |
| Governance | 3-level namespace (catalog=domain_env, schema=layer, table=entity) | ✅ |
| Governance | Managed tables, Delta format | ✅ |
| Ingestion | Auto Loader (cloudFiles, directory listing, availableNow) | ✅ |
| Ingestion | Ingestion metadata (_metadata) + rescued data | ✅ |
| Ingestion | Incremental / idempotent ingestion | ✅ |
| Bronze | Raw-fidelity ingestion | ✅ |
| Silver | Structured Streaming transformation | ✅ |
| Silver | Data quality via quarantine (6 rules, with reason) | ✅ |
| Silver | Column standardization (snake_case) | ✅ |
| Silver | Testable pure functions (extracted logic) | ✅ |
| Gold | Business aggregate table (daily metrics) | ✅ |
| Gold | Incremental upsert via MERGE | ✅ |
| Orchestration | Lakeflow Job (setup → bronze → silver → gold) | ✅ |
| Orchestration | Daily schedule (America/Sao_Paulo) | ✅ |
| Packaging | Python wheel with entry points | ✅ |
| CI/CD | Declarative Automation Bundle (dev/stg/prod) | ✅ |
| CI/CD | Environment isolation via domain_env catalogs | ✅ |
| CI/CD | Variable injection (no secrets in repo) | ✅ |
| CI/CD | Service principal for prod run identity | ✅ |
| CI/CD | GitHub Actions (validate + test + deploy) | ✅ |
| CI/CD | Unit tests gating merges | ✅ |
| CI/CD | Git Flow (feature → develop → homolog → main) | ✅ |
| CI/CD | Branch protection + prod approval gate | ✅ |

## Architecture Decisions

| ADR | Topic |
|---|---|
| 0001 | Lakeflow Jobs over Airflow |
| 0002 | Lakehouse namespace modeling (catalog per domain-environment) |

## Trial limitations

Some production practices are demonstrated in code but constrained by the Azure
Databricks trial: environments share one workspace (isolated by catalog rather
than by separate workspaces), and Unity Catalog catalogs use the metastore
default storage. See ADR-0002 for the portability note.

## Project boundary

Concepts such as CDC/MERGE from external sources, Change Data Feed, and Lakeflow
Declarative Pipelines are intentionally out of scope here, reserved for a
dedicated event-driven project (Postgres + Debezium).
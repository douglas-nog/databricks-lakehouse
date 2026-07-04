# Project Roadmap — Databricks Lakehouse (NYC Taxi)

End-to-end batch/streaming lakehouse on Azure Databricks using the medallion
architecture, Unity Catalog, and a CLI-first workflow.

Status: ✅ done · 🔶 partial · ⬜ pending

## Scope

| Area | Item | Status |
|---|---|---|
| Governance | Unity Catalog: catalog, schema, volumes | ✅ |
| Governance | 3-level namespace, managed tables | ✅ |
| Ingestion | Auto Loader (cloudFiles, directory listing, availableNow) | ✅ |
| Ingestion | Ingestion metadata + rescued data | ✅ |
| Bronze | Raw-fidelity ingestion | ✅ |
| Silver | Structured Streaming transformation | ✅ |
| Silver | Data quality via quarantine (with reason) | ✅ |
| Silver | Column standardization (snake_case) | ✅ |
| Gold | Business aggregate table | ✅ |
| Orchestration | Lakeflow Job (bronze → silver → gold) | ⬜ |
| CI/CD | Declarative Automation Bundle (dev/stg/prod) | ⬜ |
| CI/CD | GitHub Actions (validate + deploy) | ⬜ |

## Architecture Decisions

| ADR | Topic |
|---|---|
| 0001 | Lakeflow Jobs over Airflow |

## Project boundary

This project delivers a coherent batch/streaming lakehouse. Concepts such as
CDC/MERGE, Change Data Feed, and Lakeflow Declarative Pipelines are
intentionally out of scope here and will be covered in a dedicated
event-driven project (Postgres + Debezium).
# 1. Lakeflow Jobs over Airflow for intra-platform orchestration

## Status
Accepted

## Context
The medallion pipeline needs to coordinate multiple steps with dependencies
between layers, with requirements for scheduling,
retries, and monitoring. Each execution happens inside the Databricks
platform, without external systems. We have to choose an orchestration
mechanism for these dependencies.

## Decision
Use Lakeflow Jobs (native Databricks orchestration) to coordinate the
pipeline, instead of an external orchestrator such
as Apache Airflow.

## Alternatives considered
- **Apache Airflow**: A widely adopted open-source orchestration tool. It can
  coordinate a large variety of components and heterogeneous systems, which
  makes it the stronger choice when a pipeline spans multiple platforms.
- **Lakeflow Jobs**: Coordinates notebooks, LDP, and SQL queries and
  establishes dependencies between them, but only within Databricks services.
  Since this project does not integrate external systems or tools for now,
  Lakeflow Jobs is the better choice due to its lower operational overhead.

## Consequences
Only Databricks artifacts can be orchestrated. If external services are
introduced later, the orchestration layer will need to be replanned —
potentially reintroducing Airflow (or an equivalent) as a higher-level
orchestrator that triggers the Lakeflow Job as one of its steps.
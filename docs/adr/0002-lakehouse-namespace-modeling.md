# 2. Lakehouse namespace modeling: catalog per domain-environment, schema per layer

## Status
Accepted

## Context
Unity Catalog uses a three-level namespace: catalog.schema.table. The project
must map four concerns onto three levels: SDLC environments (dev, stg, prod),
business domains, medallion layers (bronze, silver, gold), and entities. The
mapping determines environment isolation, access governance, and how the model
scales as new domains and entities are added. Databricks best practices favor
catalogs for environment/domain isolation and granting access at the schema
level rather than per table.

## Decision
- **catalog** = `<domain>_<environment>` — e.g. `mobility_dev`, `mobility_stg`, `mobility_prod`
- **schema** = medallion layer — `bronze`, `silver`, `gold`
- **table** = entity — e.g. `trips`, `zones`

Example: `mobility_prod.silver.trips`

## Rationale
Catalog as `<domain>_<environment>` gives each business domain its own isolated
catalog per environment, rather than sharing a single per-environment catalog
across all domains. This isolates ownership, permissions, and blast radius by
domain, and scales to a Data Mesh model where an organization may hold hundreds
of domains, each with autonomous governance.

Schema as medallion layer aligns with the classic medallion model and with the
best practice of granting access at the schema level: consumers can be granted
the gold schema without exposing raw bronze or intermediate silver data.

Table as entity keeps `trips` as what it actually is — an entity within the
mobility domain, not a domain itself. Modeling `trips` as a catalog or schema
would conflate an entity with a business domain. The domain (`mobility`) is a
business capability; `trips`, `zones`, and `drivers` are entities within it.

## Alternatives considered
- **catalog = environment only** (`dev_catalog.trips.bronze`): simpler, common
  in smaller setups, but all domains share one per-environment catalog. This
  loses domain-level isolation of ownership and governance, and does not scale
  cleanly to many autonomous domains.
- **schema = domain, with `trips` as schema** (`catalog.trips.bronze_*`):
  conflates entity with domain and pushes the medallion layer into the table
  name, weakening schema-level governance per layer.

## Consequences
- Three catalogs must be provisioned per domain (`mobility_dev`, `mobility_stg`,
  `mobility_prod`), and this multiplies as new domains are added.
- For a single-domain project, this model can appear over-engineered. This ADR
  is the justification: the structure is chosen for scale and governance
  correctness, and demonstrates the intended enterprise model even while only
  one domain (`mobility`) currently exists.
- Volumes and pipeline control metadata (checkpoints, schemas) need a defined
  home in this model — resolved separately when refactoring setup.
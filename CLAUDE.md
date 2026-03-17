# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

postgres_connector — shared PostgreSQL access library for the soviar-systems ecosystem.
Provides `PostgresConnector` (DDL + DML operations), `ensure_schema()` (first-run
detection), and `MyQuery` (optional interactive analysis helper, pandas-dependent).
Published to PyPI; all ecosystem DB projects install it as a dependency, not a local copy.

This package does NOT contain application logic — it is a generic access layer. Project-specific
behaviour lives in each consumer project's own subclass or pipeline code.

## Governing Ecosystem Rules

<!-- Inline summaries — no links to parent dirs (standalone cloning compatibility) -->

### Commits
```
<type>: <subject ≤50 chars>

- <Verb>: <file-path> — <what and why>
```
Verbs: `Created`, `Updated`, `Deleted`, `Renamed`, `Fixed`, `Moved`, `Added`, `Removed`,
`Refactored`, `Configured`

### Package manager
`uv` only. Never call `pip` directly.

### Containers
Podman only. Kube YAML manifests via `podman play kube`, runnable via `systemctl --user`.
Never Docker, never Docker Compose, never Podman Compose.

### Safe git commands
- `git restore <file>` — never `git checkout -- <file>`
- `git restore --staged <file>` — never `git reset HEAD <file>`
- `git switch <branch>` — never `git checkout <branch>`
- Never `git reset --hard`, `git push --force`, or `git clean -f` without explicit request

### Plan management
Save plans to `misc/plan/YYYY-MM-DD_<slug>.md` before starting. Move to
`misc/plan/implemented/` when done. Track tech debt in `misc/plan/techdebt.md`.

### Contract docstrings (all files)
Every module opens with a docstring covering:
- **Scope / responsibility**: one sentence on what this file does and what it does NOT do
- **Public interface**: what callers may use; mark internals as private
- **Key design decisions**: the *why* behind non-obvious choices
- **Dependencies**: imports, env vars, external services

Test files additionally: what belongs here vs what does not, naming convention in use.

### TDD
Red → Green → Refactor. Pure-logic tests in `test_<module>.py` (no DB, runs anywhere).
Integration tests in `test_integration.py` — mark `@pytest.mark.integration`, skip unless
`TEST_DB_URL` is set.

### Non-brittle tests
Test contracts, not implementation details. Assert on exit codes, return types, side
effects — not exact message strings. Use semantic assertions (`assert len(errors) > 0`).
Parameterize varied scenarios. Each test class must have a docstring stating the contract
it verifies.

### SVA check
Before adding anything: is this proportionate to the goal? Reuse before writing.
If the implementation seems disproportionate, step back and look for a simpler approach.

### Design principles
- **Reuse before writing** — check if existing code already handles the need before adding
  new functions or classes
- **Question the plan** — if the implementation seems disproportionate to the goal, step
  back and look for a simpler approach
- **No duplication in docs** — use cross-references to the authoritative source

### DB access (ADR-26050, planned)
This package IS the shared DB access layer for the ecosystem. Downstream projects install
it as a PyPI dep; local re-implementation of DB access logic is prohibited in consumers.
All parameterised SQL uses psycopg's `sql` module — no f-string SQL, no string
concatenation for identifiers or values. Exception: DDL where every interpolated value
comes exclusively from `schemata.py` and passes through the appropriate `sql` wrapper.

### Schema namespacing (ADR-26051, planned)
Each DB project uses its own PostgreSQL schema. Schema name is passed to
`PostgresConnector` at instantiation via `schema=info.schema`. All generated SQL uses
two-part `sql.Identifier(schema, table)`. `ensure_schema()` creates the schema before
any table DDL.

### Logging
`logging.getLogger(__name__)` in every module. No `print()` in production code.

### Secrets (ADR-26053, planned)
No credentials in code or committed env files. CLI tools receive secrets via env vars
injected by the secrets manager at runtime. No `pexpect`.

## Ecosystem ADR Index

https://github.com/soviar-systems/ai_engineering_handbook/blob/main/architecture/adr_index.md

## Architecture

Implementation decisions live in `architecture/adr/`. The spoke ADR for schema definition
format (`_is_constraint()`, tuple format, `create_attributes_dict()`) is planned as
`architecture/adr/adr_001_schema_definition_format.md` — to be written alongside ADR-26050.

Key internal decisions (until the spoke ADR exists):
- **Schema tuple format:** `{table: [(col, type), ..., (CONSTRAINT_TYPE, CONSTRAINT_DEF)]}`.
  Last entry is always a constraint. Tuples keep name and type separate so
  `sql.Identifier(name)` applies without string parsing.
- **`_is_constraint()`:** checks `attr.split()[0].upper() in _CONSTRAINT_KEYWORDS`.
  Callers MUST use this function — never `[:-1]` positional slicing.
- **`create_attributes_dict()`:** builds `{table: [col_name, ...]}` by filtering out
  constraints via `_is_constraint()`. Used by pipelines to build INSERT column lists.

## Commands

```bash
uv sync                              # install deps
uv run pytest                        # run all tests
uv run pytest -m "not integration"   # skip DB tests
uv build                             # build package
```

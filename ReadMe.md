# postgres_connector

A psycopg 3 wrapper that provides schema-aware DDL and DML helpers for PostgreSQL projects.

## Installation

```bash
pip install postgres_connector

# With pandas support (MyQuery.get_df / get_list):
pip install postgres_connector[analysis]
```

## Project setup: credentials via `info.py`

The recommended pattern is to keep credentials in a small `info.py` at your
project root that reads from environment variables and falls back to local
defaults for development.

> **Security:** the hardcoded fallback values are for local development only.
> Never use real credentials as defaults, and add `info.py` to `.gitignore` if
> it ever contains non-dummy values. In production, set the env vars and leave
> the fallbacks as obvious placeholders (`"localhost"`, `"secret"`, etc.) so a
> misconfigured deployment fails visibly rather than silently connecting with
> leaked credentials.

```python
# info.py
import os

host   = os.environ.get("DB_HOST", "localhost")
port   = os.environ.get("DB_PORT", "5432")
dbname = os.environ.get("DB_NAME", "mydb")
user   = os.environ.get("DB_USER", "user1")
pwd    = os.environ.get("DB_PASSWORD", "secret")
schema = os.environ.get("DB_SCHEMA", "myschema")
```

Then instantiate `PostgresConnector` from it:

```python
import info
from postgres_connector import PostgresConnector

db = PostgresConnector(
    host=info.host, port=info.port, dbname=info.dbname,
    user=info.user, password=info.pwd,
    schema=info.schema,
)
```

This keeps credentials out of source code and makes the project portable across
environments (development, CI, containers) by setting the corresponding env vars.
In a container, inject them via the Pod/Compose env section; never bake them into
the image.

## Classes

### `PostgresConnector`

Schema-aware DDL and DML operations. All SQL is built with psycopg's `sql` module —
no f-string interpolation, no raw string concatenation.

```python
db.connect()

# First-run detection
status = db.ensure_schema(relations, check_table="resolution")
# → "created" | "empty" | "populated"

# Build column name dict (used by pipelines for INSERT column lists)
attrs = PostgresConnector.create_attributes_dict(relations)
# → {"resolution": ["id", "title_id", ...], ...}

# DML helpers
exists = db.row_exists("resolution", ["symbol"], ["A/RES/79/1"])
pk     = db.get_foreign_key("title", ["name"], ["Disarmament"])
new_id = db.insert_into_table("title", ["name"], ["Disarmament"])
# insert_into_table is get-or-create: returns PK of new or existing row

db.disconnect()
```

### `MyQuery`

Executes a SQL query at instantiation and exposes results for interactive use
(REPL, Jupyter notebooks). Not intended for production pipelines.

```python
from postgres_connector import MyQuery

q = MyQuery(
    "SELECT * FROM myschema.resolution LIMIT 10",
    host=info.host, port=info.port, dbname=info.dbname,
    user=info.user, password=info.pwd,
    schema=info.schema,
)
print(q.rows)       # list of tuples
print(q.columns)    # list of column names
q.explain()         # EXPLAIN query plan
df = q.get_df()     # pandas DataFrame (requires [analysis] extra)
```

### `MyLogger`

Configures a rotating file + console logger for projects that manage their own
logging. Projects that configure logging elsewhere (e.g. via a framework) do not
need this.

```python
from postgres_connector import MyLogger

MyLogger(log_dir="./logs", log_file="db.log", verbose=True)
# logging.getLogger("postgres_connector") now writes to ./logs/db.log
```

## Schema definition format

Define your tables as a dict of lists of tuples. The last entry of every table
list must be a constraint (`PRIMARY KEY`, `UNIQUE`, etc.):

```python
# schemata.py
relations = {
    "title": [
        ("id",   "SERIAL"),
        ("name", "TEXT UNIQUE"),
        ("PRIMARY KEY", "(id)"),    # ← constraint entry, always last
    ],
}
```

`postgres_connector` uses `_is_constraint()` internally to distinguish column entries
from constraint entries — never positional slicing.

"""
Shared PostgreSQL access library for all projects under /projects/databases/.

Design contract (ADR-001):
  - All SQL uses psycopg's sql module (sql.Identifier, sql.Literal, sql.SQL).
    No f-string or %-style SQL construction anywhere in this file.
  - Schema definitions use the tuple format: {table: [(col, type), ..., (CONSTRAINT, def)]}.
    The last entry of every table list MUST be a constraint entry (PRIMARY KEY, UNIQUE, …).
    Use _is_constraint() to detect constraint entries — never positional slicing.
  - Each project uses its own PostgreSQL schema (namespace). Pass schema= at instantiation.
    All table references are schema-qualified: sql.Identifier(self.schema, table).
  - Logging via logging.getLogger(__name__). No print() in production code.
  - No pexpect. Dump/restore operations live in start.sh using PGPASSWORD env var.

Public API:

  PostgresConnector(host, port, dbname, user, password, schema="public")
    .connect()                              → self
    .disconnect()                           → self
    .ensure_schema(relations, check_table)  → "created" | "empty" | "populated"
    .create_tables(relations)               → self
    .create_attributes_dict(relations)      → {table: [col, ...]}   (static method)
    .drop_all_tables()                      → self
    .row_exists(table, attributes, values)  → bool
    .get_foreign_key(table, attrs, vals)    → int  (raises KeyError if not found)
    .insert_into_table(table, attrs, vals)  → int  (get-or-create; returns PK)

  MyQuery(query, host, port, dbname, user, password, schema="public")
    Executes query at init; exposes .rows, .columns, .get_df(), .get_list(), .explain().
    Requires pandas for get_df() and get_list():
        pip install postgres_connector[analysis]

  MyLogger(...)
    Configures a RotatingFileHandler + StreamHandler logger for standalone (non-Scrapy)
    projects. Scrapy projects do not need this — Scrapy configures logging automatically.

See also:
  psycopg3 transactions: https://www.psycopg.org/psycopg3/docs/basic/transactions.html
  psycopg3 sql module:   https://www.psycopg.org/psycopg3/docs/api/sql.html
"""

import logging
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional

import psycopg
from psycopg import sql

try:
    import pandas as pd
    _PANDAS_AVAILABLE = True
except ImportError:
    _PANDAS_AVAILABLE = False

logger = logging.getLogger(__name__)

# ── Schema-definition helpers ─────────────────────────────────────────────────

_CONSTRAINT_KEYWORDS = frozenset({"PRIMARY", "UNIQUE", "FOREIGN", "CHECK", "EXCLUDE"})


def _is_constraint(attr: str) -> bool:
    """
    Return True if `attr` (the first element of a schemata tuple) is a SQL
    constraint keyword rather than a column name.

    Used by create_tables() and create_attributes_dict() to distinguish:
      ("id",          "SERIAL")           → column      → False
      ("PRIMARY KEY", "(id)")             → constraint  → True
      ("UNIQUE",      "(col_a, col_b)")   → constraint  → True
    """
    return attr.split()[0].upper() in _CONSTRAINT_KEYWORDS


# ── Logger helper (for standalone / non-Scrapy projects) ─────────────────────

class MyLogger:
    """
    Configure a Python logger with a rotating file handler and a console handler.

    Intended for standalone projects that manage their own logging (e.g. scripts,
    notebooks). Scrapy projects do NOT need this — Scrapy configures the root logger
    at startup and all logging.getLogger(__name__) calls in this library flow through
    it automatically.

    Usage:
        my_log = MyLogger(log_dir="./logs", log_file="db.log", verbose=True)
        db = PostgresConnector(..., ...)
        # logging.getLogger("postgres_connector") now writes to ./logs/db.log
    """

    def __init__(
        self,
        mode: str = "a",
        log_formatter: str = "[%(asctime)s] %(levelname)s: %(message)s, line %(lineno)d, in %(name)s",
        console_formatter: str = "[%(asctime)s] %(levelname)s: %(message)s",
        max_bytes: int = 20 * 1024 * 1024,  # 20 MB
        rotate_number: int = 5,
        debug: bool = False,
        verbose: bool = False,
        log_dir: Optional[str] = "./",
        log_file: Optional[str] = "database.log",
        add_date: Optional[bool] = False,
    ):
        self.log_dir = log_dir
        self.log_file = log_file
        self.add_date = add_date
        self.mode = mode
        self.log_formatter = log_formatter
        self.console_formatter = console_formatter
        self.max_bytes = max_bytes
        self.rotate_number = rotate_number
        self.debug = debug
        self.verbose = verbose
        self.logger = self._setup_logging()

    def _setup_logging(self) -> logging.Logger:
        """
        Build and return a configured logger.

        File handler (DEBUG level) is added when both log_dir and log_file are set.
        Console handler level: DEBUG (if debug=True) > INFO (if verbose=True) > ERROR.
        Debug mode takes precedence over verbose mode.

        Returns: logging.Logger
        """
        _logger = logging.getLogger(__name__)
        _logger.setLevel(logging.DEBUG)

        if _logger.hasHandlers():
            _logger.handlers.clear()

        file_fmt = logging.Formatter(self.log_formatter)

        if self.log_dir and self.log_file:
            log_dir_path = Path(self.log_dir)
            log_dir_path.mkdir(parents=True, exist_ok=True)

            log_file = self._dated_filename() if self.add_date else self.log_file
            lfh = RotatingFileHandler(
                log_dir_path / log_file,
                mode=self.mode,
                maxBytes=self.max_bytes,
                backupCount=self.rotate_number,
            )
            lfh.setLevel(logging.DEBUG)
            lfh.setFormatter(file_fmt)
            _logger.addHandler(lfh)

        console_fmt = logging.Formatter(self.console_formatter)
        ch = logging.StreamHandler()
        if self.debug:
            ch.setLevel(logging.DEBUG)
            ch.setFormatter(file_fmt)
        elif self.verbose:
            ch.setLevel(logging.INFO)
            ch.setFormatter(console_fmt)
        else:
            ch.setLevel(logging.ERROR)
            ch.setFormatter(console_fmt)
        _logger.addHandler(ch)

        return _logger

    def _dated_filename(self) -> str:
        """
        Insert a timestamp into the log file name.
        E.g. "database.log" → "database.20260317_143022.log"
        Uses %H%M%S (portable) rather than %s (Linux-only Unix timestamp).
        """
        now = datetime.now().strftime("%Y%m%d_%H%M%S")
        p = Path(self.log_file)
        return f"{p.stem}.{now}{p.suffix or '.log'}"


# ── Core database connector ───────────────────────────────────────────────────

class PostgresConnector:
    """
    psycopg 3 wrapper providing schema-aware DDL and DML operations.

    All methods that generate SQL use psycopg's sql module exclusively —
    no f-string or %-style SQL. Table names are always schema-qualified
    via sql.Identifier(self.schema, table).

    Parameters:
    - host, port, dbname, user, password: connection credentials
    - schema: str: PostgreSQL schema (namespace) for this project's tables.
      Default "public". Set per-project via info.py / DB_SCHEMA env var.
      See ADR-001 §4.
    """

    def __init__(
        self,
        host: str,
        port: str,
        dbname: str,
        user: str,
        password: str,
        schema: str = "public",
    ):
        self.host = host
        self.port = port
        self.dbname = dbname
        self.user = user
        self.password = password
        self.schema = schema
        self.connection = None
        logger.debug("PostgresConnector instance created (schema=%s)", schema)

    def connect(self, autocommit: bool = True) -> "PostgresConnector":
        """
        Open a psycopg connection and store it in self.connection.

        Parameters:
        - autocommit: bool: when True each statement commits immediately.
          Pipelines open explicit transactions via connection.transaction(),
          which works correctly under autocommit mode — see
          https://www.psycopg.org/psycopg3/docs/basic/transactions.html

        Returns self for fluent chaining. Safe to call more than once (no-op
        if already connected).
        """
        if not self.connection:
            self.connection = psycopg.connect(
                f"host={self.host} port={self.port} dbname={self.dbname}"
                f" user={self.user} password={self.password}",
                autocommit=autocommit,
                prepare_threshold=2,
            )
            logger.info('Connected to "%s"', self.dbname)
        else:
            logger.debug('Already connected to "%s"', self.dbname)
        return self

    def disconnect(self) -> "PostgresConnector":
        """
        Close the connection and reset self.connection to None.

        Returns self. Safe to call even if already disconnected.
        """
        if self.connection:
            self.connection.close()
            self.connection = None
            logger.info('Disconnected from "%s"', self.dbname)
        else:
            logger.debug("disconnect() called but no connection was open")
        return self

    def ensure_schema(
        self, relations: dict, check_table: str = "resolution"
    ) -> str:
        """
        Detect whether the schema and tables exist and have data.

        Uses the database itself as the source of truth for first-run detection —
        no log-file checks.

        Parameters:
        - relations: dict: schemata dict in tuple format (from schemata.py);
          passed to create_tables() only when the schema is missing.
        - check_table: str: table name used as the presence/population probe.
          Defaults to "resolution"; set to the main fact table of your project.

        Returns one of:
        - "created":   schema/table did not exist; create_tables() was called.
        - "empty":     table exists but has 0 rows.
        - "populated": table exists with at least one row.
        """
        try:
            count = self.connection.execute(
                sql.SQL("SELECT COUNT(*) FROM {}").format(
                    sql.Identifier(self.schema, check_table)
                )
            ).fetchone()[0]
        except psycopg.errors.UndefinedTable:
            logger.info(
                "Table %s.%s not found — creating schema...", self.schema, check_table
            )
            self.create_tables(relations)
            return "created"

        if count == 0:
            logger.info("%s.%s exists but is empty", self.schema, check_table)
            return "empty"

        logger.info("%s.%s rows: %d", self.schema, check_table, count)
        return "populated"

    def create_tables(self, relations: dict) -> "PostgresConnector":
        """
        Create the PostgreSQL schema (namespace) and all tables defined in `relations`.

        Uses psycopg's sql module throughout:
        - Column names → sql.Identifier(col_name)
        - Type definitions → sql.SQL(type_def)   (internal schemata.py strings)
        - Constraint entries → sql.SQL(keyword + " " + definition)
        - Table names → sql.Identifier(self.schema, table)

        Constraint vs column detection uses _is_constraint() — NOT positional slicing.

        Parameters:
        - relations: dict: {table_name: [(col, type), ..., (CONSTRAINT, def)]}
          See schemata.py and ADR-001 §3 for the required format.

        Returns self.
        """
        self.connection.execute(
            sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                sql.Identifier(self.schema)
            )
        )
        logger.info("Schema %s ready", self.schema)

        for table, attributes in relations.items():
            parts = []
            for attr, type_def in attributes:
                if _is_constraint(attr):
                    # e.g. ("PRIMARY KEY", "(id)") → PRIMARY KEY (id)
                    # Both tokens come from internal schemata.py — sql.SQL() is safe.
                    parts.append(sql.SQL(f"{attr} {type_def}"))
                else:
                    # e.g. ("name", "TEXT UNIQUE") → "name" TEXT UNIQUE
                    parts.append(
                        sql.SQL("{} {}").format(
                            sql.Identifier(attr),
                            sql.SQL(type_def),
                        )
                    )
            query = sql.SQL("CREATE TABLE IF NOT EXISTS {} ({})").format(
                sql.Identifier(self.schema, table),
                sql.SQL(", ").join(parts),
            )
            self.connection.execute(query)
            logger.debug(query.as_string(self.connection))

        logger.info("All tables created in schema %s", self.schema)
        return self

    @staticmethod
    def create_attributes_dict(relations: dict) -> dict:
        """
        Build a {table: [col_name, ...]} dict from a schemata relations dict.

        Constraint entries are excluded using _is_constraint() — the first token
        of the attribute name is checked against _CONSTRAINT_KEYWORDS.
        Do NOT use positional slicing ([:-1]) to exclude constraints; that breaks
        if a table ever has a column as its last entry.

        Parameters:
        - relations: dict: schemata in tuple format {table: [(col, type), ...]}

        Returns: dict[str, list[str]]
        """
        return {
            table: [attr for attr, _ in attributes if not _is_constraint(attr)]
            for table, attributes in relations.items()
        }

    def drop_all_tables(self) -> "PostgresConnector":
        """
        Drop all tables in self.schema with CASCADE.

        Intended for development resets only — never called during normal operation.

        Returns self.
        """
        if not self.connection:
            raise RuntimeError("drop_all_tables() called without an open connection")

        cursor = self.connection.execute(
            sql.SQL(
                "SELECT table_name FROM information_schema.tables"
                " WHERE table_schema = {}"
            ).format(sql.Literal(self.schema))
        )
        table_names = [row[0] for row in cursor.fetchall()]

        if table_names:
            for name in table_names:
                self.connection.execute(
                    sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(
                        sql.Identifier(self.schema, name)
                    )
                )
            logger.info("Dropped from %s: %s", self.schema, ", ".join(table_names))
        else:
            logger.info("No tables to drop in schema %s", self.schema)

        return self

    def row_exists(
        self, table: str, attributes: list, values: list
    ) -> bool:
        """
        Return True if a row matching all (attribute, value) pairs exists.

        Uses sql.Identifier for column names and sql.Literal for values —
        safe for any string content and non-string types (int, date, None).

        Parameters:
        - table: str: bare table name (schema applied automatically)
        - attributes: list[str]: column names to filter on
        - values: list: corresponding values; must be same length as attributes

        Raises ValueError if len(attributes) != len(values).
        """
        if len(attributes) != len(values):
            raise ValueError(
                f"Attribute/value count mismatch for {self.schema}.{table}: "
                f"{len(attributes)} attrs vs {len(values)} values.\n"
                f"attributes: {attributes}\nvalues: {values}"
            )

        conditions = sql.SQL(" AND ").join(
            sql.SQL("{} = {}").format(sql.Identifier(a), sql.Literal(v))
            for a, v in zip(attributes, values)
        )
        query = sql.SQL("SELECT EXISTS (SELECT 1 FROM {} WHERE {})").format(
            sql.Identifier(self.schema, table),
            conditions,
        )
        result = self.connection.execute(query).fetchone()[0]
        logger.debug("%s → %s", query.as_string(self.connection), result)
        return result

    def get_foreign_key(
        self,
        table: str,
        attributes: list,
        values: list,
        id_: str = "id",
    ) -> int:
        """
        Fetch the primary key of the row where all (attribute, value) pairs match.

        Parameters:
        - table: str: bare table name (schema applied automatically)
        - attributes: list[str]: column names to filter on
        - values: list: corresponding filter values
        - id_: str: PK column name; default "id"

        Returns: int (PK value)
        Raises KeyError if no matching row is found.
        """
        query = sql.SQL("SELECT {} FROM {} WHERE {}").format(
            sql.Identifier(id_),
            sql.Identifier(self.schema, table),
            sql.SQL(" AND ").join(
                sql.SQL("{} = {}").format(sql.Identifier(a), sql.Literal(v))
                for a, v in zip(attributes, values)
            ),
        )
        logger.debug(query.as_string(self.connection))

        cursor = self.connection.execute(query).fetchone()
        if cursor:
            logger.debug('"%s" found in %s.%s: %s', id_, self.schema, table, cursor[0])
            return cursor[0]

        raise KeyError(
            f"No row found in {self.schema}.{table} for "
            f"{dict(zip(attributes, values))}"
        )

    def insert_into_table(
        self,
        table: str,
        attributes: list,
        values: list,
        id_: str = "id",
    ) -> int:
        """
        Get-or-create: INSERT the row and return its PK; if a duplicate conflict
        occurs, fetch and return the existing row's PK.

        Strategy: INSERT ON CONFLICT DO NOTHING RETURNING {id_}.
        - New row → RETURNING yields the PK; one round-trip.
        - Existing row → RETURNING yields nothing → get_foreign_key() fetches PK;
          two round-trips only in the conflict case, which is the minority.

        Parameters:
        - table: str: bare table name (schema applied automatically)
        - attributes: list[str]: column names (must NOT include the SERIAL/PK column)
        - values: list: corresponding values, same length as attributes
        - id_: str: PK column name; default "id"

        Returns: int (PK of the new or existing row)
        Raises ValueError if len(attributes) != len(values).
        """
        if len(attributes) != len(values):
            raise ValueError(
                f"Attribute/value count mismatch for {self.schema}.{table}: "
                f"{len(attributes)} attrs vs {len(values)} values.\n"
                f"attributes: {attributes}\nvalues: {values}"
            )

        query = sql.SQL(
            "INSERT INTO {rel} ({cols}) VALUES ({vals})"
            " ON CONFLICT DO NOTHING RETURNING {id_}"
        ).format(
            rel=sql.Identifier(self.schema, table),
            cols=sql.SQL(", ").join(map(sql.Identifier, attributes)),
            vals=sql.SQL(", ").join(map(sql.Literal, values)),
            id_=sql.Identifier(id_),
        )
        logger.debug(query.as_string(self.connection))

        result = self.connection.execute(query).fetchone()
        if result:
            logger.debug('Inserted into %s.%s, %s=%s', self.schema, table, id_, result[0])
            return result[0]

        # Conflict: row already exists — fetch its PK
        logger.debug('Conflict in %s.%s — fetching existing %s', self.schema, table, id_)
        return self.get_foreign_key(table, attributes, values, id_=id_)


# ── Interactive analysis helper ───────────────────────────────────────────────

class MyQuery(PostgresConnector):
    """
    Execute a SQL query and inspect the results interactively.

    Executes `query` at instantiation time and stores rows + column names.
    Provides explain(), get_df(), get_list() for notebook / REPL use.

    get_df() and get_list() require pandas:
        pip install postgres_connector[analysis]

    Parameters: same as PostgresConnector plus:
    - query: str: raw SQL query to execute (no parameterisation — use only in
      trusted, interactive contexts, not in production pipelines).
    """

    def __init__(
        self,
        query: str,
        host: str,
        port: str,
        dbname: str,
        user: str,
        password: str,
        schema: str = "public",
    ):
        super().__init__(host, port, dbname, user, password, schema=schema)
        self.query = query
        self.connect()
        cursor = self.connection.execute(query)
        self.rows = cursor.fetchall()
        self.columns = [d[0] for d in cursor.description] if cursor.description else []

    def execute(self, query: str) -> None:
        """
        Execute a raw SQL string on the open connection and print results.

        Intended for quick ad-hoc queries in a REPL/notebook. For production
        code use the parameterised methods on PostgresConnector directly.
        """
        cursor = self.connection.execute(query)
        if cursor.description:
            cols = [d[0] for d in cursor.description]
            print("\t".join(cols))
            print("-" * 60)
            for row in cursor.fetchall():
                print("\t".join(str(v) for v in row))

    def explain(self, analyze: bool = False) -> None:
        """
        Run EXPLAIN [ANALYZE] on self.query and print the query plan.

        Parameters:
        - analyze: bool: if True, runs EXPLAIN ANALYZE (executes the query).
        """
        prefix = "EXPLAIN ANALYZE " if analyze else "EXPLAIN "
        cursor = self.connection.execute(prefix + self.query)
        print("\t", cursor.description[0][0])
        print("-" * 80)
        for row in cursor.fetchall():
            print(row[0])

    def get_df(self, index: bool = False):
        """
        Return query results as a pandas DataFrame.

        Parameters:
        - index: bool: if True, use the first column as the DataFrame index.

        Requires pandas. Raises ImportError if not installed.
        """
        if not _PANDAS_AVAILABLE:
            raise ImportError(
                "pandas is required for get_df(). "
                "Install with: pip install postgres_connector[analysis]"
            )
        df = pd.DataFrame(self.rows, columns=self.columns)
        if index and not df.empty:
            df = df.set_index(df.columns[0])
        return df

    def get_list(
        self, column_name: str, sort: bool = False, unique: bool = False
    ) -> list:
        """
        Return a list of values from a single column of the query result.

        Parameters:
        - column_name: str: must match one of self.columns
        - sort: bool: sort the result
        - unique: bool: deduplicate (uses pandas .unique())

        Requires pandas. Raises ImportError if not installed.
        """
        if not _PANDAS_AVAILABLE:
            raise ImportError(
                "pandas is required for get_list(). "
                "Install with: pip install postgres_connector[analysis]"
            )
        result = self.get_df()[column_name]
        if unique:
            result = result.unique()
        if sort:
            return sorted(result)
        return list(result)


# ── Notebook display helper ───────────────────────────────────────────────────

def view(rows: int = 5, cols: int = 15) -> None:
    """
    Configure pandas display options for comfortable notebook viewing.

    Parameters:
    - rows: int: max rows to display (default 5)
    - cols: int: max columns to display (default 15)

    Requires pandas. Raises ImportError if not installed.
    """
    if not _PANDAS_AVAILABLE:
        raise ImportError(
            "pandas is required for view(). "
            "Install with: pip install postgres_connector[analysis]"
        )
    pd.set_option("display.max_rows", rows)
    pd.set_option("display.min_rows", rows)
    pd.set_option("display.max_columns", cols)

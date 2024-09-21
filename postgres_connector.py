import logging
import re
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import List, Optional

import pandas as pd
import pexpect
import psycopg
from psycopg import sql


class MyLogger:
    """
    Define a custom logger for the database project.
    There are two possible kinds of loggers: a file logger
      and a console logger (stdin and stderr).
    Define a new instance before defining a new
      postgres_connector instance.

    By default, with each start a logger creates a file
      with the date in its name and, this log file is
      rotated each 20 MB for 100 backups which is enough
      to keep all the log data for small database projects.
    """

    def __init__(
        self,
        mode: str = "a",
        log_formatter: str = "[%(asctime)s] %(levelname)s: %(message)s, line %(lineno)d, in %(name)s",
        console_formatter: str = "[%(asctime)s] %(levelname)s: %(message)s",
        max_bytes: int = 20 * 1200 * 1200,  # 20 MB
        rotate_number: int = 5,  # i.e. a lot :)
        debug: bool = False,
        verbose: bool = False,
        log_dir: Optional[str] = "./",
        log_file: Optional[str] = "database.log",
        add_date: Optional[bool] = False,  # to log_file
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
        self.logger = self.setup_logging()

    def setup_logging(self) -> logging.Logger:
        """
        Configures logging for the database connector:
        - a file logger for saving all logs (DEBUG by default,
          hardcoded),
        - a console logger for side effect watching (ERROR
          by default, can be configured in debug and verbose
          flags).
        By default, console logger has its own simple
          formatter, while the log file is much more detailed.
        Console logger can work in debug mode (formatter is
          the same as log file), and verbose and error
          (default) modes (simple formatter).
          Debug mode has precedence over verbose mode.
        If you set log_dir OR log_file to None, only console output
          will be set as a logger.

        Parameters are set in the __init__ method.

        Returns:
        - logging.Logger: The configured logger object.
        """

        # logger configuration
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.DEBUG)

        if logger.hasHandlers():
            logger.handlers.clear()

        # log file handler
        lf = logging.Formatter(self.log_formatter)  # used in ch.setFormatter
        if self.log_dir and self.log_file:

            # Create the log directory if it doesn't exist
            log_dir_path = Path(self.log_dir)
            log_dir_path.mkdir(parents=True, exist_ok=True)

            # log_file name
            log_file = self.add_date_to_log_file() if self.add_date else self.log_file

            # Rotating file handler for database logs
            log_file_path = log_dir_path / log_file
            lfh = RotatingFileHandler(
                log_file_path,
                mode=self.mode,
                maxBytes=self.max_bytes,
                backupCount=self.rotate_number,
            )
            lfh.setLevel(logging.DEBUG)
            lfh.setFormatter(lf)

            logger.addHandler(lfh)

        # Console handler
        cf = logging.Formatter(self.console_formatter)
        ch = logging.StreamHandler()
        if self.debug:
            ch.setLevel(logging.DEBUG)
            ch.setFormatter(lf)
        else:
            if self.verbose:
                ch.setLevel(logging.INFO)
            else:  # default
                ch.setLevel(logging.ERROR)
            ch.setFormatter(cf)
        logger.addHandler(ch)

        return logger

    def add_date_to_log_file(self) -> str:
        """
        Add date to log_file name
        """
        now = datetime.now().strftime("%Y%m%d_%s")
        splitted_fname = self.log_file.split(".")

        if ".log" in self.log_file:
            return f"{'.'.join(splitted_fname[:-1])}.{now}.{splitted_fname[-1]}"
        else:
            return f"{'.'.join(splitted_fname)}.{now}.log"


class PostgresConnector:
    """
    Class that has ready-to-use methods for updating the db.
      Also you can implement any pg built-in command using
      a generic method "pg_command()"
    """

    def __init__(
        self,
        # connection configuration
        host: str,
        port: str,
        dbname: str,
        user: str,
        password: str,
        # logger
        logger: logging.Logger = None,
        # new database config
        schemata: Optional[dict] = None,  # for a brand new database
        filter_list: Optional[List[str]] = [
            "PRIMARY",
            "FOREIGN",
            "UNIQUE",
            #"id",
        ],  # for attributes_dict and schema.sql file
    ):
        # connection
        self.host = host
        self.port = port
        self.dbname = dbname
        self.user = user
        self.password = password
        self.connection = None
        # new database
        self.schemata = schemata  # to build a brand new database
        self.filter_list = filter_list
        self.attributes_dict = self.create_attributes_dict()
        # logger
        if logger is None:
            logger = MyLogger(log_dir=None).logger
        self.logger = logger
        self.logger.debug("New postgres_connector instance created")

    def create_attributes_dict(self) -> Optional[dict]:
        """
        Retrieve the attributes' names in a dictionary format.

        Parameters:
        - schemata: dict: dict with the tables' names as keys and SQL-formatted
          attributes lists of strings for each attribute as values (see example
          below);
        - filter_list: list: a list with the words, like 'primary' or 'unique'
          to filter the attribute string; notice that the method retrieves the
          first word from the attribute string, so does it check the first
          word in the filter list, not all the words in the string.

        Example of the schemata dict:
            ```
            schemata = {
                "un_resolution_title": ("id SERIAL",
                          "name TEXT UNIQUE NOT NULL",
                          "PRIMARY KEY (id)"),
                "un_meeting": ("id SERIAL",
                                   "symbol VARCHAR(64) UNIQUE NOT NULL",
                                   "PRIMARY KEY (id)")
            }
            ```
        """
        if self.schemata is None:
            return None

        # lowercase all the words in the filter_list
        filter_list = list(map(str.lower, self.filter_list))

        # our return dictionary
        attributes_dict = dict()
        for table, attributes in self.schemata.items():
            for attr in attributes:
                attr = attr.split()[0]
                if attr.lower() not in filter_list:
                    attributes_dict.setdefault(table, []).append(attr)

        self.logger.debug(f"self.filter_list = {self.filter_list}")
        self.logger.debug(f"self.attributes_dict = {attributes_dict}")

        return attributes_dict

    def pg_command(
        self,
        pg_utility: str,
        options: Optional[str] = None,
        arguments: Optional[str] = None,
    ) -> str:
        """
        Base method for executing any pg command against
          the database. Methods of manipulating the database
          themtselves are based on this method.

        Parameters:
        - pg_utility: str: utility like createdb, pg_dump, etc.;
        - options: str: options of the pg_utility;
        - arguments: str: any arguments like an SQL query.
        """
        # connection args
        args = [
            "-h",
            self.host,
            "-p",
            self.port,
            "-U",
            self.user,
            self.dbname,
        ]
        # add options
        if options:
            args.extend(options.split())
        # add arguments
        if arguments:
            self.logger.debug(
                f'Executing command: {pg_utility} {" ".join(args)} "{arguments}"'
            )
            args.append(arguments)
        else:
            self.logger.debug(f'Executing command: {pg_utility} {" ".join(args)}')

        # command execution
        child = pexpect.spawn(command=pg_utility, args=args, encoding="utf-8")
        child.expect(r".*word.*", timeout=10)
        child.sendline(self.password)

        i = child.expect([r".*word.*", pexpect.EOF], timeout=10)
        # check for second password query
        if i == 0:
            message = f"Incorrect password passed while executing {pg_utility} command."
            raise AttributeError(message)

        # result = child.before.decode()
        result = child.before

        # check for errors in result
        if "error" in result.lower():
            message = f"Unexpected error happened while executing {pg_utility} command: {result}"
            raise RuntimeError(message)

        # notify about other events
        elif "notice" in result.lower():
            self.logger.warning(result)
            return 1

        self.logger.debug(result)
        return result

    def drop_database(self, silent: bool = False) -> "PostgresConnector":
        """
        Drop entire database. All the connections are forcibly stopped.

        Parameters:
        - silent: bool: use --if-exists option to skip exception.
        """

        try:
            options = "-e -f"
            if silent:
                options += " --if-exists"
            i = self.pg_command(pg_utility="dropdb", options=options)

        except Exception as e:
            self.logger.error(f"Count not drop_database(): {e}")
            raise e

        if i != 1:
            self.logger.info(f'Database "{self.dbname}" has been successfully dropped')

        return self

    def create_database(self) -> "PostgresConnector":
        """
        Create a new empty database with the owner of the
          current database.
        """

        try:
            self.pg_command(pg_utility="createdb", options="-e")
        except Exception as e:
            self.logger.critical(f"Could not create_database(): {e}")
            raise e

        self.logger.info(f'Database "{self.dbname}" has been successfully created')
        return self

    def connect(self, autocommit: bool = True) -> "PostgresConnector":
        if not self.connection:
            self.connection = psycopg.connect(
                f"""
                    host={self.host}
                    port={self.port}
                    dbname={self.dbname}
                    user={self.user}
                    password={self.password}
                 """,
                # autocommit does not open transactions
                # if False - the entire connection will be one big transaction
                autocommit=autocommit,
                prepare_threshold=1,
            )
            self.logger.debug(
                f'Connection to "{self.dbname}" database has been established'
            )
        else:
            self.logger.debug(
                f'Connection to "{self.dbname}" database is already established'
            )
        return self

    def disconnect(self) -> "PostgresConnector":
        if self.connection:
            self.connection.close()
            self.connection = None
            self.logger.debug(f'Connection to "{self.dbname}" database has been closed')
        else:
            self.logger.debug(f'No connection to "{self.dbname}" found')

        return self

    def create_schema_file(self, fname: Optional[str] = None) -> Path:
        """
        Creates an SQL file with schemas for creating tables.

        Parameters:
        - fname: str: a name for your SQL file with the
          SQL-formatted schemata.

        Returns:
        - fname: Path object.
        """

        if fname is None:
            fname = f"{self.dbname}_schema.sql"
        fname = Path(fname)

        # establish connection if needed
        _need_to_disconnect = False
        if self.connection is None:
            _need_to_disconnect = True
            self.connect()

        with open(fname, "w") as f:
            # build each table's creation query
            for table, attributes in self.schemata.items():
                # build columns strings
                columns = list()
                for attr in attributes:
                    if attr.split()[0] not in self.filter_list:
                        columns.append(
                            sql.SQL("{} {}").format(
                                sql.Identifier(attr.split()[0]),
                                sql.SQL(" ".join(attr.split()[1:])),
                            )
                        )
                    else:
                        columns.append(sql.SQL("{}").format(sql.SQL(attr)))

                columns = [sql.SQL(",\n    ").join(columns)]
                # build a query string
                query = sql.SQL("CREATE TABLE IF NOT EXISTS {} (\n    {}\n);\n").format(
                    sql.Identifier(table), sql.SQL(",\n").join(columns)
                )
                f.write(query.as_string(self.connection))

        self.logger.info(f'Schema file saved as "{str(fname)}"')

        if _need_to_disconnect:
            self.disconnect()
        return fname

    def create_tables(
        self,
        fname: Optional[str] = None,
        keep_file: Optional[bool] = False,
    ) -> "PostgresConnector":
        """
        Creates tables in the database based on the provided schemas file.

        Parameters:
        - fname: str: the name for your SQL-formatted schemata file;
        - verbose: bool: Flag to print verbose output

        Returns:
        - self
        """

        try:
            # create an sql schema file
            fname = self.create_schema_file(fname)

            # create tables
            i = self.pg_command(
                pg_utility="psql", options="-e -f", arguments=str(fname)
            )

        except Exception as e:
            if fname:
                fname.unlink()
            message = f"Could not create_tables(): {e}"
            self.logger.critical(message)
            raise RuntimeError(message)

        if i != 1:
            tables = list(self.attributes_dict.keys())
            self.logger.info(
                "Tables {} have been successfully created".format(
                    ", ".join(f'"{t}"' for t in tables)
                )
            )
        if not keep_file:
            fname.unlink()
            self.logger.debug(f"{fname} file has been removed")

        return self

    def drop_all_tables(self):
        """
        Drops all the tables in the database.
        """
        if self.connection is None:
            message = (
                f"Could not drop_all_tables(): self.connection is {self.connection}"
            )
            self.logger.critical(message)
            raise RuntimeError(message)
        # get the relations' names
        query = sql.SQL(
            """
            SELECT table_name
              FROM information_schema.tables
              WHERE table_schema = 'public'
            """
        )
        self.logger.debug(query.as_string(self.connection))
        cursor = self.connection.execute(query)
        tables = [table[0] for table in cursor.fetchall()]

        # delete tables
        if tables:
            for table in tables:
                query = sql.SQL(
                    """
                    DROP TABLE IF EXISTS {} CASCADE;
                    """
                ).format(sql.Identifier(table))
                self.connection.execute(query)

            self.logger.info(
                "Tables {} have been successfully dropped".format(
                    ", ".join([f'"{t}"' for t in tables])
                )
            )
        else:
            self.logger.info("No tables to drop found")

        return self

    def make_dump(self, options="-v -f", fpath="./", fname=None) -> str:
        """
        Make the database dump and save the file in the
          fname path. Built-in psql command is used;
          you are free to modify its behaviour
        Parameters:
        - fpath: str: where to save the dump; './' by default;
        - fname: str: file name for dump.

        Returns:
        - str: a path of the dump file.
        """

        if fname is None:
            fname = f"{self.dbname}_{datetime.now().strftime('%Y%m%d_%s')}.sql"

        if "/" not in fpath:
            fpath += "/"
        arguments = f"{fpath}{fname}"
        self.pg_command(pg_utility="pg_dump", options=options, arguments=arguments)

        self.logger.info(f'"{self.dbname}" database dump saved as {arguments}.')

        return arguments

    def export_schema(self, options="-s -F p -E UTF-8 -f", fpath="./", fname=None):
        """
        Export the schemata of the existing database for
          creating beautiful schema image on
          https://dbdiagram.io/d

        Parameters:
        - fname: str: a file name;
        - fpath: str: a file path, default is './'.

        Returns:
        - fname as Path object
        """

        if fname is None:
            fname = f"{self.dbname}_schema.sql"

        self.make_dump(options=options, fpath=fpath, fname=fname)
        return fname

    def row_exists(self, table: str, attributes: list[str], values: list[str]) -> bool:
        """
        Checks whether the entire row is already in the table.

        Parameters:
        - table: str: table name;
        - attributes: list;
        - values: list;

        Returns:
        - bool: True if the row exists
        """

        if len(attributes) != len(values):
            self.logger.error(
                f"Number of attributes ({len(attributes)}) and values ({len(values)}) is different.\nAttributes: {attributes}\nValues: {values}\nSee row_exists() method."
            )
            raise ValueError

        query = sql.SQL("SELECT EXISTS (SELECT 1 FROM {} WHERE {})").format(
            sql.Identifier(table),
            sql.SQL(" AND ").join(
                [
                    sql.SQL("{} = {}").format(sql.Identifier(attr), sql.Literal(val))
                    for attr, val in zip(attributes, values)
                ]
            ),
        )

        result = self.connection.execute(query).fetchone()[0]

        self.logger.debug(query.as_string(self.connection))
        self.logger.info(f"Row exists: {result}")

        return result

    def get_foreign_key(
        self,
        table: str,
        attributes: list[str],
        values: list[any],
        id_: str = "id",
        no_log: bool = False,
    ) -> int:
        """
        Get the primary key of the table's tuple.

        Parameters:
        - table: str
        - attributes: list of str
        - value: list of values of any type
        - id_: str: name for id attribute if it differs from
          cannonical "id"
        - no_log: bool: set True if you want to supress
          logging.

        Returns:
        - id: int
        """
        if not self.row_exists(table, attributes, values):
            if not no_log:
                self.logger.error(
                    f'No data found for "{table} table" while executing get_foreign_key method. Foreign key cannot be obtained'
                )
                self.logger.debug(f"Attributes: {attributes}, values: {values}.")
            raise AttributeError

        self.logger.info(f'Getting "{id_}" from "{table}"...')
        query = sql.SQL("SELECT {} FROM {} WHERE {};").format(
            sql.Identifier(id_),
            sql.Identifier(table),
            sql.SQL(" AND ").join(
                [
                    sql.SQL("{} = {}").format(sql.Identifier(attr), sql.Literal(val))
                    for attr, val in zip(attributes, values)
                ]
            ),
        )

        self.logger.debug(query.as_string(self.connection))

        cursor = self.connection.execute(query).fetchone()
        if cursor:
            fk = cursor[0]
            self.logger.info(f'"{id_}" found: {fk}')
            return fk

        self.logger.error(
            f'fetchone() returned "{cursor}" while getting the "{id_}" from "{table}" table while executing get_foreign_key() method'
        )
        raise KeyError

    def insert_into_table(
        self, table: str, attributes: list[str], values: list[str], id_: str = "id"
    ) -> int:
        """
        Inserts data into attributes of the table and returns
          the primary key, i.e. id.

        Parameters:
        - table: str: Name of the table
        - attributes: list or tuple of strings: List of attribute names
        - values: list or tuple of strings: List of
          corresponding values for attributes
        - special_id: str: specify the name for id attribute
          if it is different from cannonical "id"

        Returns:
        - id of the the row, i.e. its primary key.
        """
        if len(attributes) != len(values):
            self.logger.error(
                f'Number of attributes and values in "{table}" is different\nAttributes: {attributes}\nValues: {values}'
            )
            raise ValueError

        try:
            self.logger.info(f'Trying to insert new data into "{table}" table...')
            pk = self.get_foreign_key(table, attributes, values, id_=id_, no_log=True)
            # self.logger.info(f'The value(s) is(are) already in the "{table}" table.')
            return pk

        except:
            self.logger.info(f'Populating "{table}" table...')

        query_string = "INSERT INTO {rel} ({cols}) VALUES ({vals}) ON CONFLICT DO NOTHING RETURNING {id_};"

        query = sql.SQL(query_string).format(
            rel=sql.Identifier(table),
            cols=sql.SQL(", ").join(map(sql.Identifier, attributes)),
            vals=sql.SQL(", ").join(map(sql.Literal, values)),
            id_=sql.Identifier(id_),
        )

        self.logger.debug(query.as_string(self.connection))

        result = self.connection.execute(query).fetchone()
        if result:
            self.logger.info(f'Insertion into "{table}" table is successful')
            return result[0]

    def copy_csv(self, table, attributes, data, verbose=False):
        """
        Loads raw csv into one PostgeSQL table.

        Parameters:
        - table: str: name of the table to copy to
        - attributes: list: list of attributes of the table to copy to
        - data: object: csv reader object
        - connection: psycopg class
        """

        cols_for_query = ", ".join(attributes)
        query = f"COPY {table} ({cols_for_query}) FROM STDIN"
        count = 0

        if verbose:
            print("Started copying the csv...")
        # this way is much faster but the result is written at the end
        for row in data:
            copy.write_row(row)
            count += 1
            if count % 100_000 == 0:
                print(f"{count} rows loaded...")
        # if count == 100_000:
        #     break

        # this way is much much slower but it writes each row at a time
        # for row in data:
        #     with connection.cursor().copy(query) as copy:
        #         copy.write_row(row)

        if verbose:
            print(f'"{table}" has been loaded into database "{self.dbname}"')

        return 0


class MyQuery(PostgresConnector):
    """
    Provide a query and get all you need for manipulations.
    There are two primary ways of retrieving data from the db:
    1. Using psycopg lib: you get data you can manipulate with;
    2. Using psql: you get only side effect, use when you need
      to execute built-in psql commands.
    WARNING: No SQL injection protection, direct communication to the database.
    """

    def __init__(
        self,
        query: Optional[str] = None,
        logger: Optional[logging.Logger] = None,
        host: str = info.host,
        port: str = info.port,
        dbname: str = info.dbname,
        user: str = info.user,
        password: str = info.pwd,
    ):
        """
        init cursor and rows ready for use
        """
        super().__init__(logger, host, port, dbname, user, password)
        self.query = query
        # if you just want to see the credentials, like dbname, this is skipped
        if self.query:
            if "\\" not in self.query:
                self.cursor = self.get_cursor()
                self.rows = self.cursor.fetchall()
                self.columns = [desc[0] for desc in self.get_cursor().description]

    def get_cursor(self, query: Optional[str] = None) -> psycopg.cursor:
        """
        Get psycopg cursor, method is used in init
        """
        if query == None:
            query = self.query
        with psycopg.connect(
            f"""
            host={self.host}
            port={self.port}
            dbname={self.dbname}
            user={self.user}
            password={self.password}
            """
        ) as conn:
            return conn.execute(query)

    def psql(self) -> None:
        """
        Implements psql query in terminal and prints psql view of data
        """
        result = self.pg_command("psql", "-c", self.query)
        print(result)
        # return result
        # query = f'psql -h {self.host} -p {self.port} -U {self.user} -d {self.dbname} -c "{query}"'

        # child = pexpect.spawn(query)
        ## child.expect(r".*word*")
        # child.expect(f".*{self.user}.*")
        # child.sendline(self.password)
        # child.expect(pexpect.EOF)

        # print the query result
        # print(child.before.decode())

    def explain(self, command="EXPLAIN ", verbose=False):
        """
        Method calls the built-in pg explain method on the query
        """
        query = command + self.query
        if verbose:
            print(query)
        cursor = self.get_cursor(query)

        print("\t", cursor.description[0][0])
        print("-" * 80)

        rows = cursor.fetchall()
        for line in rows:
            print(line[0])

    def explain_analyze(self, verbose=False):
        """
        Method calls the built-in pg explain and
          analyze methods on the query
        """
        self.explain(command="EXPLAIN ANALYZE ", verbose=verbose)

    def get_df(self, index=False):
        """
        Create pandas dataframe from the queried data

        Parameters:
        - index: bool: whether to use the first column as index;
        """
        try:
            # Get column names
            columns = [desc[0] for desc in self.cursor.description]
            # print(self.cursor.description)
        except Exception as e:
            print("ERROR in self.get_df:", e)
            return 1

        # Create a DataFrame from the fetched rows
        result = pd.DataFrame(self.rows, columns=columns)

        if index:
            result = result.set_index(result.iloc[:, 0])
            result = result.drop(result.columns[0], axis=1)

        return result

    def get_list(self, column_name: str, sort=False, unique=False):
        """
        Get list of values from the given column
        """

        result = self.get_df()[column_name]
        if unique:
            result = result.unique()

        if sort:
            return sorted(result)

        return list(result)


def view(rows=5, cols=15):
    """
    DataFrame rows and columns view for database queries
    """
    pd.set_option("display.max_rows", rows)
    pd.set_option("display.min_rows", rows)
    pd.set_option("display.max_columns", cols)

import psycopg2
import subprocess
import datetime
from git_data_ops.src.core.config import PostgresConfig
from git_data_ops.src.database.abstract_database import (
    AbstractDatabase,
)
import os
from contextlib import contextmanager
import re
from git_data_ops.src.logger import get_logger
from typing import Tuple, List, Dict


logger = get_logger(__file__)


class PostgresConnector(AbstractDatabase):
    def __init__(self, config: PostgresConfig):
        self.config: PostgresConfig = config
        self.table_instances: Dict[str, PostgresTable] = {}

    def test_connection(self):
        conn = psycopg2.connect(self.config._connection_string)
        print("Connection succesful")

    @property
    def _connection(self):
        conn = psycopg2.connect(self.config._connection_string)
        return conn

    @contextmanager
    def cursor(self, autocommit: bool = False) -> psycopg2.extensions.cursor:
        conn = self._connection
        conn.autocommit = autocommit
        cur = conn.cursor()
        try:
            yield cur
            conn.commit()
        except psycopg2.DatabaseError:
            logger.error("Database operation failed:", exc_info=True, stack_info=True)
            raise
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def get_cursor_columns(cursor):
        return [col.name for col in cursor.description]

    def list_databases(self):
        with self.cursor() as cur:
            cur.execute("SELECT datname FROM pg_database;")
            databases = [
                db[0] for db in cur.fetchall() if not re.match(r"template\d+", db[0])
            ]

        return databases

    def get_tables(self) -> List[dict]:
        with self.cursor() as cur:
            get_tables_query = """
                SELECT *
                FROM information_schema.tables
                WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
            """
            cur.execute(get_tables_query)
            table_rows = cur.fetchall()
            columns = self.get_cursor_columns(cur)

        result_dict = self.tuple_list_to_dict(keys=columns, tuple_list=table_rows)

        return result_dict

    def get_columns_for_table(
        self, table_catalog: str, table_schema: str, table_name: str
    ) -> List[dict]:
        with self.cursor() as cur:
            get_columns_query = f"""
            SELECT *
            FROM information_schema.columns
            WHERE table_catalog = '{table_catalog}'
            AND table_schema = '{table_schema}'
            AND table_name = '{table_name}'
        """
            cur.execute(get_columns_query)
            column_rows = cur.fetchall()
            columns = self.get_cursor_columns(cur)

        result_dict = self.tuple_list_to_dict(keys=columns, tuple_list=column_rows)

        return result_dict

    def get_keys_for_table(
        self, table_catalog: str, table_schema: str, table_name: str
    ) -> List[dict]:
        with self.cursor() as cur:
            get_columns_query = f"""
            SELECT kcu.*,
                   tc.constraint_type
            FROM information_schema.key_column_usage kcu
            JOIN information_schema.table_constraints tc
            ON kcu.constraint_name = tc.constraint_name
            WHERE kcu.table_catalog = '{table_catalog}'
            AND kcu.table_schema = '{table_schema}'
            AND kcu.table_name = '{table_name}'
        """
            cur.execute(get_columns_query)
            keys_rows = cur.fetchall()
            columns = self.get_cursor_columns(cur)

        result_dict = self.tuple_list_to_dict(keys=columns, tuple_list=keys_rows)

        return result_dict

    def branch_has_feature_db(self, database_name: str, branch: str):
        databases = self.list_databases()

        if database_name not in databases:
            raise KeyError(f"Database {database_name} not in {self.config.host}")

        separators = ["-", "_", "/", ":"]
        matches = []

        for database in databases:
            for separator in separators:
                if database == database_name + separator + branch:
                    matches.append(database)

        if matches:
            return True
        else:
            return False

    def dump_database(self, globals: bool = True):
        self._create_dump_dir_if_not_exists()

        dump_destination_file = (
            datetime.datetime.now().strftime("%Y-%m-%d_%H:%M:%S") + "_dump.sql"
        )
        dump_destination_file = os.path.join("dumps", dump_destination_file)

        globals_destination_file = (
            datetime.datetime.now().strftime("%Y-%m-%d_%H:%M:%S") + "_globals.sql"
        )
        globals_destination_file = os.path.join(
            "dumps", "globals", globals_destination_file
        )

        logger.info(
            f"Dumping database {self.config.database} to {dump_destination_file}"
        )
        try:
            process = subprocess.Popen(
                [
                    "pg_dump",
                    "--dbname=postgresql://{}:{}@{}:{}/{}".format(
                        self.config.user,
                        self.config.password,
                        self.config.host,
                        self.config.port,
                        self.config.database,
                    ),
                    "-f",
                    dump_destination_file,
                ],
                stdout=subprocess.PIPE,
            )
            output = process.communicate()[0]
            if process.returncode != 0:
                print("Command failed. Return code : {}".format(process.returncode))
                exit(1)

        except Exception as e:
            print(e)
            exit(1)

        if globals:
            try:
                # Environment variables to bypass auth prompt
                os.environ["PGPASSWORD"] = self.config.password
                process = subprocess.Popen(
                    [
                        "pg_dumpall",
                        "-h",
                        self.config.host,
                        "-p",
                        str(self.config.port),
                        "-U",
                        self.config.user,
                        "-g",
                        "-f",
                        globals_destination_file,
                    ],
                    stdout=subprocess.PIPE,
                )
                # import sys; sys.exit()
                output = process.communicate()[0]
                if process.returncode != 0:
                    print("Command failed. Return code : {}".format(process.returncode))
                    exit(1)
                return output
            except Exception as e:
                print(e)
                exit(1)
            finally:
                del os.environ["PGPASSWORD"]

    def recover_database(self, database: str, dump_filename: str):
        existing_databases = self.list_databases()
        if database not in existing_databases:
            with self.cursor(autocommit=True) as cur:
                cur.execute(f'CREATE DATABASE "{database}"')
        else:
            logger.error(f"Database {database} already exists. Exiting...")
            return

        logger.info(f"Recovering database {database} from {dump_filename}")
        try:
            process = subprocess.Popen(
                [
                    "psql",
                    "--dbname=postgresql://{}:{}@{}:{}/{}".format(
                        self.config.user,
                        self.config.password,
                        self.config.host,
                        self.config.port,
                        database,
                    ),
                    "-f",
                    dump_filename,
                ],
                stdout=subprocess.PIPE,
            )
            output = process.communicate()[0]
            if process.returncode != 0:
                print("Command failed. Return code : {}".format(process.returncode))
                exit(1)

        except Exception as e:
            print(e)
            exit(1)

    def get_all_tables(self):
        tables = self.get_tables()
        for table in tables:
            self.table_instances[
                f'{table["table_schema"]}.{table["table_name"]}'
            ] = PostgresTable(
                connector=self,
                table_catalog=table["table_catalog"],
                table_schema=table["table_schema"],
                table_name=table["table_name"],
                table_type=table["table_type"],
            )

        return self.table_instances


class PostgresTable:
    def __init__(
        self,
        connector: PostgresConnector,
        table_catalog,
        table_schema,
        table_name,
        table_type,
        columns=None,
        keys=None,
    ):
        self.connector = connector
        self.table_catalog = table_catalog
        self.table_schema = table_schema
        self.table_name = table_name
        self.table_type = table_type
        self.columns = columns or self.fetch_columns_from_db()
        self.keys = keys or self.fetch_keys_from_db()

    def fetch_columns_from_db(self):
        columns_dict = self.connector.get_columns_for_table(
            table_catalog=self.table_catalog,
            table_schema=self.table_schema,
            table_name=self.table_name,
        )

        return columns_dict

    def fetch_keys_from_db(self):
        keys_dict = self.connector.get_keys_for_table(
            table_catalog=self.table_catalog,
            table_schema=self.table_schema,
            table_name=self.table_name,
        )

        return keys_dict

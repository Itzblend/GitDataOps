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


class PostgresConnector(AbstractDatabase):
    def __init__(self, config: PostgresConfig):
        self.config: PostgresConfig = config

    def test_connection(self):
        conn = psycopg2.connect(self.config._connection_string)
        print("Connection succesful")

    @property
    def connection(self):
        conn = psycopg2.connect(self.config._connection_string)
        return conn

    @contextmanager
    def cursor(self) -> psycopg2.extensions.cursor:
        try:
            conn = psycopg2.connect(self.config._connection_string)
            cursor = conn.cursor()
            yield cursor
        except psycopg2.Error as e:
            print("Error: Could not connect to database.")
            print(e)
        finally:
            conn.commit()
            conn.close()

    def list_databases(self):
        with self.cursor() as cur:
            cur.execute("SELECT datname FROM pg_database;")
            databases = [
                db[0] for db in cur.fetchall() if not re.match(r"template\d+", db[0])
            ]

        return databases

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

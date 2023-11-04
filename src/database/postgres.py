import psycopg2
import subprocess
import datetime
from src.core.config import PostgresConfig
from src.database.abstract_database_connector import AbstractDatabaseConnector
import os


class PostgresConnector(AbstractDatabaseConnector):
    def __init__(self, config: PostgresConfig):
        self.config: PostgresConfig = config

    def test_connection(self):
        conn = psycopg2.connect(self.config._connection_string)
        print("Connection succesful")

    def dump_database(self, globals: bool = True):
        self.create_dump_dir_if_not_exists()

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

from abc import ABC, abstractmethod
from ruamel.yaml import YAML
import os
from datetime import datetime

yaml = YAML(typ="safe")


class ConfigManager(ABC):
    def __init__(self, config_file_path: str = "config/database.yml"):
        self.config_file_path = config_file_path
        self.config = self.load_config()
        self.databases = {"postgresql": PostgresConfig}

    def load_config(self):
        with open(self.config_file_path, "r", encoding="utf-8") as config_file:
            return yaml.load(config_file)

    def get_database_config(self, database_identifier, branch):
        if database_identifier in self.config["databases"]:
            database_type = self.config["databases"][database_identifier]["type"]
            branch = self.config["branch"]
            if database_type in self.databases:
                return self.databases[database_type](database_identifier, branch)
            raise KeyError(f"Database type {database_type} not in available options")
        raise KeyError(f"Database {database_identifier} not found in config")

    def list_latest_dumps(self, directory: str, n: int = 5):
        # Get a list of files in the directory
        files = os.listdir(directory)

        # Create a list of tuples with filenames and their modification timestamps
        file_timestamps = [
            (file, mtime_to_datetime(os.path.getmtime(os.path.join(directory, file))))
            for file in files
            if os.path.isfile(os.path.join(directory, file))
        ]

        # Sort the list based on modification timestamps in descending order (latest first)
        file_timestamps.sort(key=lambda x: x[1], reverse=True)

        # Print or use the 5 latest modified files
        latest_files = file_timestamps[:n]

        return latest_files


def mtime_to_datetime(mtime):
    return datetime.fromtimestamp(mtime)


class PostgresConfig(ConfigManager):
    def __init__(self, config_identifier: str, branch: str):
        super().__init__()
        self.database_config = self.config["databases"][config_identifier]["branches"][
            branch
        ]
        self.type = self.config["databases"][config_identifier]["type"]
        self.host = self.config["databases"][config_identifier]["branches"][branch][
            "host"
        ]
        self.port = self.config["databases"][config_identifier]["branches"][branch][
            "port"
        ]
        self.user = self.config["databases"][config_identifier]["branches"][branch][
            "username"
        ]
        self.password = self.config["databases"][config_identifier]["branches"][branch][
            "password"
        ]
        self.database = self.config["databases"][config_identifier]["branches"][branch][
            "database"
        ]

    @property
    def _connection_string(self):
        return f"host={self.host} port={self.port} user={self.user} password={self.password} dbname={self.database}"

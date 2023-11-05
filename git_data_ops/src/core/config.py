from abc import ABC, abstractmethod
from ruamel.yaml import YAML

yaml = YAML(typ="safe")


class ConfigManager(ABC):
    def __init__(self, config_file_path: str = "config/database.yml"):
        self.config_file_path = config_file_path
        self.config = self.load_config()
        self.databases = {"postgresql": PostgresConfig}

    def load_config(self):
        with open(self.config_file_path, "r", encoding="utf-8") as config_file:
            return yaml.load(config_file)

    def get_database_config(self, database_identifier):
        if database_identifier in self.config["databases"]:
            database_type = self.config["databases"][database_identifier]["type"]
            if database_type in self.databases:
                return self.databases[database_type](database_identifier)
            raise KeyError(f"Database type {database_type} not in available options")
        raise KeyError(f"Database {database_identifier} not found in config")


class PostgresConfig(ConfigManager):
    def __init__(self, config_identifier: str):
        super().__init__()
        self.database_config = self.config["databases"][config_identifier]
        self.type = self.database_config["type"]
        self.host = self.config["databases"][config_identifier]["host"]
        self.port = self.config["databases"][config_identifier]["port"]
        self.user = self.config["databases"][config_identifier]["username"]
        self.password = self.config["databases"][config_identifier]["password"]
        self.database = self.config["databases"][config_identifier]["database"]

    @property
    def _connection_string(self):
        return f"host={self.host} port={self.port} user={self.user} password={self.password} dbname={self.database}"

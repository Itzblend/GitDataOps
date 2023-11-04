from abc import ABC, abstractmethod
from ruamel.yaml import YAML

yaml = YAML(typ="safe")


class AbstractDatabaseConfig(ABC):
    def __init__(self, config_file_path: str = "config/database.yml"):
        with open(config_file_path, "r", encoding="utf-8") as config_file:
            self.config = yaml.load(config_file)


class PostgresConfig(AbstractDatabaseConfig):
    def __init__(self, config_identifier: str):
        super().__init__()
        self.database_config = self.config["databases"][config_identifier]
        self.host = self.config["databases"][config_identifier]["host"]
        self.port = self.config["databases"][config_identifier]["port"]
        self.user = self.config["databases"][config_identifier]["username"]
        self.password = self.config["databases"][config_identifier]["password"]
        self.database = self.config["databases"][config_identifier]["database"]

    @property
    def _connection_string(self):
        return f"host={self.host} port={self.port} user={self.user} password={self.password} dbname={self.database}"

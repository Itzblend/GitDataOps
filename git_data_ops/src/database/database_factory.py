from abc import ABC, abstractmethod
from git_data_ops.src.database.postgres import PostgresConnector
from typing import Dict, Any


class DatabaseFactory:
    @staticmethod
    def create_database(db_config):
        databases = {"postgresql": PostgresConnector(db_config)}
        database_type = db_config.type

        if database_type in databases.keys():
            return databases[database_type]
        raise KeyError(f"Database type {database_type} not in supported options")

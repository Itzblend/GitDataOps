import os
import shutil
from abc import ABC, abstractmethod
from typing import List, Tuple, Dict, Any
from git_data_ops.src.paths import CONFIG_PATH
from ruamel.yaml import YAML
from git_data_ops.src.core.util import write_yaml, filter_dict_by_keys
from io import StringIO

yaml = YAML(typ="safe")


class AbstractDatabase(ABC):
    @staticmethod
    def _create_dump_dir_if_not_exists():
        os.makedirs(os.path.join("dumps", "globals"), exist_ok=True)

    @staticmethod
    def tuple_list_to_dict(
        keys: List[str], tuple_list: List[Tuple]
    ) -> List[Dict[str, Any]]:
        """
        Creates a dictionary out of cursor results.
        HOX!! Keys list shouldnt be re-ordered from the original
        """
        result_dict = []

        for row in tuple_list:
            row_dict = {}
            for key, value in zip(keys, row):
                row_dict[key] = value
            result_dict.append(row_dict)

        return result_dict

    def write_state_file(self):
        state_dict = {}
        state_dict[self.config.database] = {}
        for table_identifier, table_instance in self.table_instances.items():
            table_config = {
                "table_catalog": table_instance.table_catalog,
                "table_schema": table_instance.table_schema,
                "table_name": table_instance.table_name,
                "table_type": table_instance.table_type,
                "keys": table_instance.keys,
            }
            state_dict[self.config.database][table_identifier] = table_config
        write_yaml(f"{CONFIG_PATH}/state.yml", state_dict)
        print("State file written")

    def create_ddl_script(self, table_identifier: str = None):
        if table_identifier:
            table_instances = dict(
                filter(filter_dict_by_keys, self.table_instances.items())
            )
        else:
            table_instances = self.table_instances

        for _table_identifier, table_instance in table_instances.items():
            if table_instance.table_type == "BASE TABLE":
                self.create_table_ddl_script(table_instance)

        print("Done")

    def create_table_ddl_script(self, table_instance):
        ddl_string = StringIO()

        ddl_string.write(
            f"CREATE TABLE IF NOT EXISTS {table_instance.table_schema}.{table_instance.table_name} (\n"
        )
        for column in table_instance.columns:
            ddl_string.write(f"{column['column_name']} {column['data_type']},\n")
        print(ddl_string.getvalue())

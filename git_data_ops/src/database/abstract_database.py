import os
import shutil
from abc import ABC, abstractmethod
from typing import List, Tuple
from git_data_ops.src.paths import CONFIG_PATH
from ruamel.yaml import YAML

yaml = YAML(typ="safe")


class AbstractDatabase(ABC):
    @staticmethod
    def _create_dump_dir_if_not_exists():
        os.makedirs(os.path.join("dumps", "globals"), exist_ok=True)

    @staticmethod
    def tuple_list_to_dict(keys: List[str], tuple_list: List[Tuple]) -> dict:
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
        with open(f"{CONFIG_PATH}/state.yml", "w") as state_file:
            for table_identifier, table_instance in self.table_instances.items():
                table_config = {
                    "table_catalog": table_instance.table_catalog,
                    "table_schema": table_instance.table_schema,
                    "table_name": table_instance.table_name,
                    "table_type": table_instance.table_type,
                    "keys": table_instance.keys,
                }
                yaml.dump(table_config, state_file)

        print("State file written")

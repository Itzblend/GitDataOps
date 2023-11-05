import os
import shutil
from abc import ABC, abstractmethod


class AbstractDatabase(ABC):
    @staticmethod
    def _create_dump_dir_if_not_exists():
        os.makedirs(os.path.join("dumps", "globals"), exist_ok=True)
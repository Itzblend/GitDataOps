from git import Repo
from git.exc import InvalidGitRepositoryError
from typing import Union
import os
from git_data_ops.src.logger import get_logger

logger = get_logger(__file__)


class Repository(Repo):
    def __init__(self, directory_path: str = "./"):
        if ".git" not in os.listdir(directory_path):
            raise InvalidGitRepositoryError(
                f"Git project not initialized in '{os.path.abspath(directory_path)}'"
            )
        super().__init__(directory_path)

    def list_branches(self):
        branches = self.branches

        return [branch.name for branch in branches]

    def check_branch_has_feature_database(self):
        pass

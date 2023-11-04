from git import Repo


class Repository(Repo):
    def __init__(self, directory_path: str = "./"):
        super().__init__(directory_path)

    def list_branches(self):
        return [branch.name for branch in self.branches]

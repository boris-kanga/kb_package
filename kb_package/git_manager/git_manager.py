import os

from git import Repo


class GitManager:

    def __init__(self, user=None, pwd=None, token=None):
        self.token = token
        self.user = user
        self.pwd = pwd

    def clone(self, repo_name, repo_user=None, dst=os.getcwd()):
        if repo_user is None:
            repo_user = self.user
        Repo.clone_from(
            url=f"https://{self.user}:{self.token or self.pwd}"
                f"@github.com/{repo_user}/{repo_name}.git",
            to_path=dst)

        return True


if __name__ == '__main__':
    tok = ""

    GitManager(user="boris-kanga", token=tok).clone(
        "kb_package",
        dst=r"C:\Users\kanga\OneDrive\MY-CLOUD\DOSSIER_TRAVAIL\OWN\kb_package"
    )

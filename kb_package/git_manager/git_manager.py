# Required gitPython
import os

from git import Repo


class GitManager:

    def __init__(self, user=None, pwd=None, token=None):
        self.token = token or os.environ.get("GIT_TOKEN")
        self.user = user or os.environ.get("GIT_USER")
        self.pwd = pwd or os.environ.get("GIT_PWD")

    def clone(self, repo_name, repo_user=None, dst=os.getcwd()):
        if repo_user is None:
            repo_user = self.user
        protocol = "https://"
        if self.user is not None and (self.token or self.pwd) is not None:
            protocol += f"{self.user}:{self.token or self.pwd}@"
        url = f"{protocol}github.com/{repo_user}/{repo_name}.git"
        Repo.clone_from(
            url=url,
            to_path=dst)

        return True


if __name__ == '__main__':
    tok = "token"
    git_user = "user"
    repo = "repo"

    dst = os.getcwd()

    GitManager(user=git_user,
               token=tok
               ).clone(
        repo,
        dst=dst
    )

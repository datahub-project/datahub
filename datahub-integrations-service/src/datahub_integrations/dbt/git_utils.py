import contextlib
import hashlib
import pathlib
from typing import Iterator

import git.exc
import github.Repository
import pydantic
import pygitops.operations
from datahub.configuration.common import ConfigModel
from datahub.configuration.validate_field_rename import pydantic_renamed_field
from git import Actor, Repo
from github import Auth, Github
from loguru import logger
from pygitops._util import checkout_pull_branch
from pygitops.exceptions import PyGitOpsError
from pygitops.operations import (
    feature_branch,
    get_default_branch,
    get_updated_repo,
    stage_commit_push_changes,
)


class GitHubAppAuth(ConfigModel):
    app_id: str  # can actually also be client ID
    private_key: pydantic.SecretStr
    installation_id: int

    def make_github_auth(self) -> Auth.AppInstallationAuth:
        # This currently doesn't request specific token permissions,
        # and assumes that the app was installed with the right permissions.
        return Auth.AppAuth(
            self.app_id, self.private_key.get_secret_value()
        ).get_installation_auth(self.installation_id)


class GitHubRepoConfig(ConfigModel):
    github_repo_org: str
    github_repo_name: str
    auth: pydantic.SecretStr | GitHubAppAuth

    _rename_token_to_auth = pydantic_renamed_field("token", "auth")

    def make_github_auth(self) -> Auth.Auth:
        if isinstance(self.auth, GitHubAppAuth):
            return self.auth.make_github_auth()

        return Auth.Token(self.auth.get_secret_value())


class GitHubRepoWrapper:
    def __init__(self, config: GitHubRepoConfig, base_temp_dir: pathlib.Path):
        self.config = config
        self.repo_dir = base_temp_dir / self._make_unique_clone_suffix(config)

        # See https://stackoverflow.com/questions/66908568/how-to-correctly-set-a-github-app-committer-to-show-the-app-created-the-commit
        # Currently using https://api.github.com/users/acryl-data%5Bbot%5D
        self._actor = Actor(
            "acryl-data[bot]", "131486190+acryl-data[bot]@users.noreply.github.com"
        )

        self._auth = self.config.make_github_auth()
        self._github_api = Github(auth=self._auth)

        self._most_recent_token: str | None = None
        self._repo: Repo | None = None
        assert self.repo  # force repo to be initialized

    @property
    def repo(self) -> Repo:
        # TODO: I'm not super happy with complex logic in a property getter.
        # However, it's the most effective way to ensure that the repo's auth
        # is always set up correctly without requiring the user to explicitly
        # call a explicit "refresh" method.

        # While auth.token looks like a property access, it automatically refreshes the token
        # if it's expired.
        current_token = self._auth.token
        current_clone_url = self._make_remote_url(self.config, token=current_token)

        if self._repo is None:
            if self.repo_dir.exists() and pygitops.operations._is_git_repo(
                self.repo_dir
            ):
                logger.info(
                    f"Repo {self.get_owner_and_repo()} already cloned, pulling latest changes."
                )

                # Because of the base_temp_dir caching behavior, we need to update
                # the origin url on a seemingly "fresh" clone.
                repo = Repo(self.repo_dir)
                repo.git.remote("set-url", "origin", current_clone_url)
            else:
                # This is the first time we're accessing the repo, so we need to clone it.
                logger.info(
                    f"Cloning repo {self.get_owner_and_repo()} to {self.repo_dir}"
                )

            self._repo = get_updated_repo(
                repo_url=current_clone_url,
                clone_dir=self.repo_dir,
                force=True,
            )

        elif self._most_recent_token != current_token:
            # The token has changed, so we need to update the origin URL.
            logger.info(f"Updating origin URL for repo {self.get_owner_and_repo()}")
            self._repo.git.remote("set-url", "origin", current_clone_url)

        assert self._repo is not None
        self._most_recent_token = current_token
        return self._repo

    @classmethod
    def _make_remote_url(cls, config: GitHubRepoConfig, token: str) -> str:
        return f"https://git:{token}@github.com/{config.github_repo_org}/{config.github_repo_name}.git"

    @classmethod
    def _make_unique_clone_suffix(cls, config: GitHubRepoConfig) -> str:
        remote_url = cls._make_remote_url(config, token="***")

        suffix = hashlib.sha256()
        suffix.update(remote_url.encode())
        return f"{config.github_repo_name}-{suffix.digest().hex()[:16]}"

    def get_owner_and_repo(self) -> str:
        return f"{self.config.github_repo_org}/{self.config.github_repo_name}"

    @contextlib.contextmanager
    def feature_branch(self, branch_name: str) -> Iterator[None]:
        with feature_branch(self.repo, branch_name):
            yield

    @contextlib.contextmanager
    def long_running_branch(self, branch_name: str) -> Iterator[None]:
        # The `feature_branch` context manager throws an error if the branch already exists.
        # We want to reuse the branch if it already exists.
        try:
            default_branch = self.get_default_branch()
            self.repo.heads[default_branch].checkout(force=True)

            # TODO: This makes the assumption that the branch is deleted when the PR is merged.
            # If that's not the case, then we need to check for open PRs first and use the branch
            # only if the associated PR is open.
            origin = self.repo.remotes.origin
            origin.fetch(prune=True)

            checkout_pull_branch(self.repo, branch_name, force=True)

            yield
        except git.exc.GitCommandError as e:
            if "fatal: couldn't find remote ref" in str(e):
                # The branch was deleted on the remote, but still exists locally.
                # In this case, we can delete the local branch and start over.
                logger.info(
                    f"Branch {branch_name} not found in remote but exists locally. "
                    "Deleting the local branch and starting over from the main branch."
                )

                # If we're currently on the feature branch due to the checkout_pull_branch call,
                # we need to move off of it in order to delete it.
                self.repo.heads[default_branch].checkout(force=True)
                self.repo.git.branch("-D", branch_name)

                with feature_branch(self.repo, branch_name):
                    yield
            elif "fatal: Need to specify how to reconcile divergent branches" in str(e):
                # This happens when we created a commit locally but failed to push it, and
                # a commit was added to the remote branch concurrently.
                # See https://jvns.ca/blog/2024/02/01/dealing-with-diverged-git-branches/
                # In our case, we should discard the local changes by deleting our copy
                # of the branch, and pull from the remote.
                logger.warning(
                    f"Branch {branch_name} has diverged. Deleting local branch and "
                    "pulling from remote."
                )

                self.repo.heads[default_branch].checkout(force=True)
                self.repo.git.branch("-D", branch_name)

                checkout_pull_branch(self.repo, branch_name, force=True)
                yield
            else:
                raise e
        except PyGitOpsError:
            # If the branch doesn't exist in the remote, we can just
            # create a new branch locally.
            with feature_branch(self.repo, branch_name):
                yield

    def stage_commit_and_push(
        self,
        branch_name: str,
        commit_message: str,
        force: bool = False,
    ) -> None:
        stage_commit_push_changes(
            self.repo,
            branch_name,
            self._actor,
            commit_message=commit_message,
            kwargs_to_push=dict(force_with_lease=True) if force else {},
        )

    def get_default_branch(self) -> str:
        return get_default_branch(self.repo)

    def force_checkout(self, branch_name: str) -> None:
        self.repo.git.checkout(branch_name, force=True)

        # Clean up any untracked files.
        self.repo.git.clean("-df")

    def upsert_github_pr(
        self,
        title: str,
        body_for_create: str,
        body_for_append: str,
        branch: str,
        draft: bool = False,
    ) -> None:
        """Create or update a GitHub PR for the current branch."""

        owner = self.config.github_repo_org
        repo: github.Repository.Repository = self._github_api.get_repo(
            self.get_owner_and_repo(), lazy=True
        )

        base_branch = self.get_default_branch()

        # Check if the PR already exists.
        prs = repo.get_pulls(
            state="open",
            head=f"{owner}:{branch}",
        )
        if prs.totalCount >= 1:
            if prs.totalCount > 1:
                logger.warning(
                    f"Found {prs.totalCount} PRs for branch {branch}, using the first one."
                )
            pr = next(iter(prs))

            current_body = pr.body
            updated_body = f"{current_body}{body_for_append}"

            logger.info(f"PR already opened, editing instead: {pr.html_url}")
            pr.edit(title=title, body=updated_body)
        else:
            # If it doesn't exist, create it.
            pr = repo.create_pull(
                title=title,
                body=body_for_create,
                head=branch,
                base=base_branch,
                draft=draft,
            )
            logger.info(f"Created PR {pr.html_url}")

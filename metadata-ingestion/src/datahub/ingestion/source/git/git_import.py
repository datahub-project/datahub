import logging
import os
import pathlib
from pathlib import Path
from typing import Optional
from uuid import uuid4

import git
from git.util import remove_password_if_present
from pydantic import SecretStr

logger = logging.getLogger(__name__)


class GitClone:
    def __init__(self, tmp_dir: str, skip_known_host_verification: bool = True):
        self.tmp_dir = tmp_dir
        self.skip_known_host_verification = skip_known_host_verification
        self.last_repo_cloned: Optional[git.Repo] = None

    def clone(
        self, ssh_key: Optional[SecretStr], repo_url: str, branch: Optional[str] = None
    ) -> Path:
        # Note: this does a shallow clone.

        unique_dir = str(uuid4())
        keys_dir = f"{self.tmp_dir}/{unique_dir}/keys"
        checkout_dir = f"{self.tmp_dir}/{unique_dir}/checkout"
        dirs_to_create = [keys_dir, checkout_dir]
        for d in dirs_to_create:
            os.makedirs(d, exist_ok=True)

        # Write the SSH key to a file.
        if ssh_key is not None:
            git_ssh_identity_file = os.path.join(keys_dir, "ssh_key")
            with open(
                git_ssh_identity_file,
                "w",
                opener=lambda path, flags: os.open(path, flags, 0o600),
            ) as fp:
                fp.write(ssh_key.get_secret_value())
                # SSH keys must have a trailing newline. Multiple newlines are fine,
                # so we can just add one unconditionally.
                fp.write("\n")
        else:
            git_ssh_identity_file = None

        # Clone the repo using the ssh key.
        git_ssh_cmd = "ssh"
        if git_ssh_identity_file:
            git_ssh_cmd += f" -i {git_ssh_identity_file}"
        if self.skip_known_host_verification:
            # Without this, the ssh command will prompt for confirmation of the host key.
            # See https://stackoverflow.com/a/28527476/5004662.
            git_ssh_cmd += (
                " -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no"
            )
        logger.debug(f"ssh_command={git_ssh_cmd}")

        if branch is None:
            logger.info(
                f"⏳ Cloning repo '{self.sanitize_repo_url(repo_url)}' (default branch), this can take some time..."
            )
            self.last_repo_cloned = git.Repo.clone_from(
                repo_url,
                checkout_dir,
                env=dict(GIT_SSH_COMMAND=git_ssh_cmd),
                depth=1,
            )
        else:
            # Because we accept branch names, tags, and commit hashes in the branch parameter,
            # we can't just use the --branch flag of Git clone. Doing a blobless clone allows
            # us to quickly checkout the right commit.
            logger.info(
                f"⏳ Cloning repo '{self.sanitize_repo_url(repo_url)}' (branch: {branch}), this can take some time..."
            )
            self.last_repo_cloned = git.Repo.clone_from(
                repo_url,
                checkout_dir,
                env=dict(GIT_SSH_COMMAND=git_ssh_cmd),
                filter="blob:none",
            )
            logger.info(f"Checking out branch {branch}")
            self.last_repo_cloned.git.checkout(branch)

        logger.info("✅ Cloning complete!")
        return pathlib.Path(checkout_dir)

    def get_last_repo_cloned(self) -> Optional[git.Repo]:
        return self.last_repo_cloned

    @staticmethod
    def sanitize_repo_url(repo_url: str) -> str:
        """Sanitizes the repo URL for logging purposes.

        Args:
            repo_url (str): The repository URL.

        Returns:
            str: The sanitized repository URL.

        Examples:
            >>> GitClone.sanitize_repo_url("https://username:password@github.com/org/repo.git")
            'https://*****:*****@github.com/org/repo.git'

            >>> GitClone.sanitize_repo_url("https://github.com/org/repo.git")
            'https://github.com/org/repo.git'

            >>> GitClone.sanitize_repo_url("git@github.com:org/repo.git")
            'git@github.com:org/repo.git'
        """

        return remove_password_if_present([repo_url])[0]

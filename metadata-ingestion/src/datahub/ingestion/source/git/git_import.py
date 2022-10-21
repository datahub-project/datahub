import logging
import os
import pathlib
from pathlib import Path
from typing import Optional
from uuid import uuid4

import git
from pydantic import SecretStr

logger = logging.getLogger(__name__)


class GitClone:
    def __init__(self, tmp_dir: str, skip_known_host_verification: bool = True):
        self.tmp_dir = tmp_dir
        self.skip_known_host_verification = skip_known_host_verification
        self.last_repo_cloned: Optional[git.Repo] = None

    def clone(self, ssh_key: Optional[SecretStr], repo_url: str) -> Path:
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
        logger.debug("ssh_command=%s", git_ssh_cmd)
        logger.info(f"⏳ Cloning repo '{repo_url}', this can take some time...")
        self.last_repo_cloned = git.Repo.clone_from(
            repo_url,
            checkout_dir,
            env=dict(GIT_SSH_COMMAND=git_ssh_cmd),
        )
        logger.info("✅ Cloning complete!")
        return pathlib.Path(checkout_dir)

    def get_last_repo_cloned(self) -> Optional[git.Repo]:
        return self.last_repo_cloned

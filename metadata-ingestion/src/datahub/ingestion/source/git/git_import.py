import logging
import os
import pathlib
from pathlib import Path
from uuid import uuid4

import git
from pydantic import SecretStr

logger = logging.getLogger(__name__)


class GitClone:
    def __init__(self, tmp_dir):
        self.tmp_dir = tmp_dir

    def clone(self, ssh_key: SecretStr, repo_url: str) -> Path:
        unique_dir = str(uuid4())
        keys_dir = f"{self.tmp_dir}/{unique_dir}/keys"
        checkout_dir = f"{self.tmp_dir}/{unique_dir}/checkout"
        dirs_to_create = [keys_dir, checkout_dir]
        for d in dirs_to_create:
            os.makedirs(d, exist_ok=True)
        git_ssh_identity_file = os.path.join(keys_dir, "ssh_key")
        with open(
            git_ssh_identity_file,
            "w",
            opener=lambda path, flags: os.open(path, flags, 0o600),
        ) as fp:
            fp.write(ssh_key.get_secret_value())

        git_ssh_cmd = f"ssh -i {git_ssh_identity_file}"
        logger.debug("ssh_command=%s", git_ssh_cmd)
        logger.info(f"⏳ Cloning repo '{repo_url}', this can take some time...")
        git.Repo.clone_from(
            repo_url,
            checkout_dir,
            env=dict(GIT_SSH_COMMAND=git_ssh_cmd),
        )
        logger.info("✅ Cloning complete!")
        return pathlib.Path(checkout_dir)

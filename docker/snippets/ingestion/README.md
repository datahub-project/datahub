# Snippets used by bundled venvs unified build

These files are used when building the **bundled ingestion venvs** (full and slim). The Dockerfile copies them into the image and invokes the build script.

| File                             | Purpose                                                                                                                                                         |
| -------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `build_bundled_venvs_unified.py` | Creates per-plugin venvs (e.g. `s3-bundled`, `file-bundled`) and installs acryl-datahub + acryl-datahub-cloud with constraints and overrides.                   |
| `build_bundled_venvs_unified.sh` | Wrapper that sets env (e.g. `BUNDLED_CLI_VERSION`, `BUNDLED_VENV_PLUGINS`) and runs the Python script.                                                          |
| `constraints.txt`                | **Single source of truth for CVE minimums and shared pins.** Used by the bundled venv build and by Dockerfile pip install steps that install from requirements. |

The Dockerfile copies `constraints.txt` to `$DATAHUB_BUNDLED_VENV_PATH` for the bundled venvs; it may also be copied to the app directory for main-venv installs.

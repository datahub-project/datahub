# Virtual Environment Fix Guide

## Problem

If you encounter import errors like:

- `ModuleNotFoundError: No module named '_cffi_backend'`
- `ModuleNotFoundError: No module named 'pyarrow.lib'`
- `ModuleNotFoundError: No module named 'pydantic_core._pydantic_core'`
- `ImportError: cannot import name 'cygrpc' from 'grpc._cython'`
- `ImportError: The scipy install you are using seems to be broken`

This means the compiled C/C++ extension modules in your `.venv` are corrupted or incompatible with your system.

## Root Cause

This typically happens when:

1. Python packages with compiled extensions were installed from a cached pip build
2. The system was upgraded (OS or Python version) but the cached builds are incompatible
3. The virtual environment was copied or moved between different systems
4. The installation was interrupted or incomplete

## Quick Fix

Run the automated fix script:

```bash
cd datahub-executor
source .venv/bin/activate
./scripts/fix_compiled_extensions.sh
```

This script will:

1. Reinstall all packages with compiled extensions using `--no-cache-dir` flag
2. Force fresh compilation of all C/C++ extensions for your system
3. Fix common version conflicts (like `pytz`)

## Manual Fix

If you prefer to fix individual packages:

```bash
cd datahub-executor
source .venv/bin/activate

# Example: Fix numpy and pandas
pip install --force-reinstall --no-cache-dir numpy==1.26.4 pandas==2.1.4

# Example: Fix cryptography
pip install --force-reinstall --no-cache-dir cffi==1.17.1 cryptography==44.0.0
```

## Nuclear Option

If the fix script doesn't work, recreate the virtual environment from scratch:

```bash
cd datahub-executor
rm -rf .venv
python3.11 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

## Prevention

To avoid this issue in the future:

- Always use `--no-cache-dir` when installing packages with compiled extensions after system upgrades
- Don't copy `.venv` directories between machines
- Recreate virtual environments after major Python or OS upgrades

## Packages with Compiled Extensions

The following packages have C/C++ compiled extensions and are most likely to break:

- **Core**: `cffi`, `cryptography`, `numpy`, `pandas`
- **Scientific**: `scipy`, `scikit-learn`, `pyarrow`
- **NLP**: `spacy`, `blis`, `cymem`, `murmurhash`, `preshed`, `srsly`, `thinc`
- **Networking**: `grpcio`, `pydantic-core`, `lz4`, `pyzmq`
- **System**: `greenlet`, `psutil`

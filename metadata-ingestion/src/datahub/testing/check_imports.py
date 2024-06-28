import pathlib
from typing import List


def ensure_no_indirect_model_imports(dirs: List[pathlib.Path]) -> None:
    # This is a poor-man's implementation of a import-checking linter.
    # e.g. https://pypi.org/project/flake8-custom-import-rules/
    # If our needs become more complex, we should move to a proper linter.
    denied_imports = {
        "src.": "datahub.*",
        "datahub.metadata._schema_classes": "datahub.metadata.schema_classes",
        "datahub.metadata._urns": "datahub.metadata.urns",
    }
    ignored_files = {
        "datahub/metadata/schema_classes.py",
        "datahub/metadata/urns.py",
        "datahub/testing/check_imports.py",
    }

    for dir in dirs:
        for file in dir.rglob("*.py"):
            if any(str(file).endswith(ignored_file) for ignored_file in ignored_files):
                continue

            with file.open() as f:
                for line in f:
                    if "import" not in line:
                        continue
                    for denied_import, replacement in denied_imports.items():
                        if denied_import in line:
                            raise ValueError(
                                f"Disallowed import found in {file}: `{line.rstrip()}`. "
                                f"Import from {replacement} instead."
                            )

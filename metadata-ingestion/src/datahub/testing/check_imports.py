import pathlib
import re
from typing import List


def ensure_no_indirect_model_imports(dirs: List[pathlib.Path]) -> None:
    # This is a poor-man's implementation of a import-checking linter.
    # e.g. https://pypi.org/project/flake8-custom-import-rules/
    # If our needs become more complex, we should move to a proper linter.
    denied_imports = {
        "src.": "datahub.*",
        "datahub.metadata._internal_schema_classes": "datahub.metadata.schema_classes",
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


def ban_direct_datahub_imports(dirs: List[pathlib.Path]) -> None:
    # We also want to ban all direct imports of datahub.
    # The base `datahub` package is used to export public-facing classes.
    # If we import it directly, we'll likely end up with circular imports.

    banned_strings = [
        r"^import datahub[\s$]",
        r"^from datahub import",
    ]
    ignored_files = {
        __file__,
    }
    for dir in dirs:
        for file in dir.rglob("*.py"):
            if str(file) in ignored_files:
                continue

            file_contents = file.read_text()

            for banned_string in banned_strings:
                if re.search(banned_string, file_contents, re.MULTILINE):
                    raise ValueError(
                        f"Disallowed bare datahub import found in {file}. "
                        f"Do not import datahub directly; instead import from the underlying file."
                    )

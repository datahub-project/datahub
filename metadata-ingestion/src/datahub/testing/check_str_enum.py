import pathlib
from typing import List


def ensure_no_enum_mixin(dirs: List[pathlib.Path]) -> None:
    # See the docs on the StrEnum implementation for why this is necessary.

    bad_lines = {
        "(str, Enum)",
        "(str, enum.Enum)",
        # We don't have any int enums right now, but this will catch them if we add some.
        "(int, Enum)",
        "(int, enum.Enum)",
    }

    ignored_files = {
        "datahub/utilities/str_enum.py",
        "datahub/testing/check_str_enum.py",
    }

    for dir in dirs:
        for file in dir.rglob("*.py"):
            if any(str(file).endswith(ignored_file) for ignored_file in ignored_files):
                continue

            with file.open() as f:
                for line in f:
                    if any(bad_line in line for bad_line in bad_lines):
                        raise ValueError(
                            f"Disallowed enum mixin found in {file}: `{line.rstrip()}`. "
                            "This enum mixin's behavior changed in Python 3.11, so it will work inconsistently across versions."
                            "Use datahub.utilities.str_enum.StrEnum instead."
                        )

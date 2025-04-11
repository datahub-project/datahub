import argparse
import pathlib
import re


def update_python_versions(
    directory: pathlib.Path,
    new_version: str,
    expected_update_count: int,
) -> None:
    # This should update version numbers in
    # - pyproject.toml
    # - src/{package_name}/__init__.py
    # - src/{package_name}/_version.py
    # - NOT in setup.py

    updates = 0

    # Update pyproject.toml
    updates += _update_file(
        directory / "pyproject.toml",
        r'^version = "([^"]+)"',
        replacement=f'version = "{new_version}"',
    )

    # Update .py files.
    py_version_pattern = r'^__version__ = "([^"]+)"'
    py_updated_version = f'__version__ = "{new_version}"'
    for package_dir in directory.glob("src/*"):
        if not package_dir.is_dir():
            continue

        updates += _update_file(
            package_dir / "__init__.py", py_version_pattern, py_updated_version
        )
        updates += _update_file(
            package_dir / "_version.py", py_version_pattern, py_updated_version
        )

    if updates != expected_update_count:
        raise ValueError(
            f"Expected to make {expected_update_count} updates, but made {updates} updates"
        )


def _update_file(
    file_path: pathlib.Path,
    pattern: str,
    replacement: str,
    error_if_no_version: bool = False,
) -> int:
    if not file_path.exists():
        return 0

    existing_content = file_path.read_text()

    def replace(match):
        # Ensure that the version was unset before - we should never be updating a version that's already set.
        if match.group(1) != "1!0.0.0.dev0":
            raise ValueError(f"Version {match.group(1)} already set in {file_path}")

        return replacement

    new_content, num_updates = re.subn(
        pattern, replace, existing_content, flags=re.MULTILINE
    )
    if num_updates == 0 and error_if_no_version:
        raise ValueError(f"No version found in {file_path}")

    file_path.write_text(new_content)
    print(f"Made {num_updates} updates to {file_path}")

    return num_updates


def main():
    parser = argparse.ArgumentParser(
        description="Update version strings in python packages"
    )
    parser.add_argument(
        "--version",
        help="New version string",
        required=True,
    )
    parser.add_argument(
        "--directory",
        help="Path to directory containing python packages",
        type=str,
        required=True,
    )
    parser.add_argument(
        "--expected-update-count",
        help="Expected number of updates",
        type=int,
        required=True,
    )

    args = parser.parse_args()

    update_python_versions(
        directory=pathlib.Path(args.directory),
        new_version=args.version,
        expected_update_count=args.expected_update_count,
    )


if __name__ == "__main__":
    main()

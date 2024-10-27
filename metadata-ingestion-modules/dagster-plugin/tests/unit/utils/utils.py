import pathlib
from typing import Sequence

from datahub.testing.compare_metadata_json import assert_metadata_files_equal

try:
    from pytest import Config as PytestConfig  # type: ignore[attr-defined]
except ImportError:
    # Support for pytest 6.x.
    from _pytest.config import Config as PytestConfig  # type: ignore

__all__ = ["PytestConfig"]


def check_golden_file(
    pytestconfig: PytestConfig,
    output_path: pathlib.Path,
    golden_path: pathlib.Path,
    ignore_paths: Sequence[str] = (),
) -> None:
    update_golden = pytestconfig.getoption("--update-golden-files")

    assert_metadata_files_equal(
        output_path=output_path,
        golden_path=golden_path,
        update_golden=update_golden,
        copy_output=False,
        ignore_paths=ignore_paths,
        ignore_order=True,
    )

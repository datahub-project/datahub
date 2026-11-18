import sys
from pathlib import Path

import pytest
from packaging.markers import Marker

# Add scripts directory to path so we can import check_wheel_coverage
sys.path.append(str(Path(__file__).parent.parent))

import check_wheel_coverage

_ALL = {11, 12, 13}


def test_installed_minors_follows_python_markers():
    assert check_wheel_coverage.installed_minors(
        Marker("python_full_version < '3.13'"), _ALL
    ) == {11, 12}
    assert check_wheel_coverage.installed_minors(
        Marker("python_full_version == '3.13.*'"), _ALL
    ) == {13}


def test_installed_minors_requires_every_minor_without_a_marker():
    assert check_wheel_coverage.installed_minors(None, _ALL) == _ALL


def test_installed_minors_keeps_platform_only_markers():
    # A Windows-only package is still installed on every minor, just not on Linux.
    assert (
        check_wheel_coverage.installed_minors(Marker("sys_platform == 'win32'"), _ALL)
        == _ALL
    )


def _write_lock(directory: Path, packages: list) -> None:
    blocks = []
    for name, version, tags in packages:
        wheels = ",\n".join(
            f'    {{ url = "https://files.example/{name}-{version}-{tag}.whl" }}'
            for tag in tags
        )
        blocks.append(
            f'[[package]]\nname = "{name}"\nversion = "{version}"\nwheels = [\n{wheels}\n]\n'
        )
    (directory / "uv.lock").write_text("\n".join(blocks))


def test_main_reports_only_gaps_on_minors_the_package_is_installed_on(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
) -> None:
    cp311_cp312 = [
        "cp311-cp311-manylinux_2_17_x86_64",
        "cp312-cp312-manylinux_2_17_x86_64",
    ]
    _write_lock(
        tmp_path,
        [
            # Forked off 3.13 by its marker: no cp313 wheel is fine.
            ("forkedpkg", "1.0", cp311_cp312),
            # Installed on 3.13 too: the missing cp313 wheel is a gap.
            ("gappkg", "2.0", cp311_cp312),
        ],
    )
    (tmp_path / "constraints.txt").write_text(
        "forkedpkg==1.0 ; python_full_version >= '3.11' and python_full_version < '3.13'\n"
        "    # via acryl-datahub\n"
        "gappkg==2.0 ; python_full_version >= '3.11' and python_full_version < '3.14'\n"
    )
    monkeypatch.setattr(check_wheel_coverage, "METADATA_INGESTION_DIR", tmp_path)

    assert check_wheel_coverage.main() == 1
    out = capsys.readouterr().out
    assert "gappkg==2.0: no wheels for 3.13" in out
    assert "forkedpkg" not in out

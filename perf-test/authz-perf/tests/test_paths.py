from pathlib import Path

from lib.paths import default_fixture_dir, resolve_datapack_dir

HARNESS_ROOT = Path(__file__).resolve().parent.parent


def test_default_fixture_dir() -> None:
    fixture_dir = default_fixture_dir(HARNESS_ROOT)
    assert fixture_dir.name == "fixture"
    assert (fixture_dir / "personas.json").is_file()
    assert (fixture_dir / "benchmarks.json").is_file()


def test_resolve_datapack_dir_finds_static_assets_sibling() -> None:
    datapack_dir = resolve_datapack_dir(HARNESS_ROOT)
    assert datapack_dir is not None
    assert datapack_dir.name == "authz-perf-medium"
    assert (datapack_dir / "index.json").is_file()

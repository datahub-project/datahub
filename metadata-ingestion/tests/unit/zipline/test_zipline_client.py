import json
import os
import pathlib

from datahub.ingestion.source.zipline.client import ZiplineRepositoryReader
from datahub.ingestion.source.zipline.constants import GROUP_BYS_DIR
from datahub.ingestion.source.zipline.report import ZiplineSourceReport


def test_reader_skips_unreadable_and_schema_invalid_files(
    tmp_path: pathlib.Path,
) -> None:
    """A file that isn't valid JSON and one that is valid JSON but doesn't match
    the GroupBy schema are both skipped with distinct warnings, not crashes."""
    group_bys = tmp_path / "group_bys" / "team"
    group_bys.mkdir(parents=True)
    (group_bys / "broken.json").write_text("{not valid json")
    # Valid JSON, but metaData must be an object — this trips schema validation.
    (group_bys / "wrong_schema.json").write_text(json.dumps({"metaData": 123}))

    reader = ZiplineRepositoryReader(str(tmp_path), ZiplineSourceReport())

    assert list(reader.read_group_bys()) == []
    assert len(reader.report.unparseable_files) == 2
    titles = {warning.title for warning in reader.report.warnings}
    assert "Unreadable compiled config" in titles
    assert "Unparseable compiled config" in titles


def test_reader_autodetects_compiled_dir_at_repo_root(
    tmp_path: pathlib.Path,
) -> None:
    """A repo root resolves to its `production/` or `compiled/` output dir."""
    for compiled_dir in ("production", "compiled"):
        repo = tmp_path / compiled_dir
        (repo / compiled_dir / GROUP_BYS_DIR / "team").mkdir(parents=True)

        reader = ZiplineRepositoryReader(str(repo), ZiplineSourceReport())

        assert os.path.basename(reader.root) == compiled_dir
        assert reader.is_valid()

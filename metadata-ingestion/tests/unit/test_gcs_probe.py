from typing import Dict, List, Optional, Tuple

import pytest

from datahub.ingestion.agent.filter_check import FilterCheckResult, check_filters

HMAC: Dict[str, object] = {"hmac_access_id": "id", "hmac_access_secret": "secret"}


def _recipe(*includes: str, **spec_extra: object) -> Dict[str, object]:
    return {
        "credential": HMAC,
        "path_specs": [{"include": inc, **spec_extra} for inc in includes],
    }


def _verdicts(
    recipe: Dict[str, object], kind: str, names: List[str]
) -> Tuple[FilterCheckResult, Dict[str, Tuple[bool, Optional[str]]]]:
    result = check_filters(
        source_type="gcs", config_dict=recipe, kind=kind, parent_path=[], names=names
    )
    return result, {r.name: (r.included, r.excluded_by) for r in result.results}


def test_tables_are_judged_by_path_specs_with_gs_targets() -> None:
    result, verdicts = _verdicts(
        _recipe(
            "gs://my-bucket/data/{table}/*.parquet",
            tables_filter_pattern={"deny": ["^users$"]},
        ),
        "Table",
        [
            "gs://my-bucket/data/events",
            "gs://my-bucket/data/users",
            "gs://my-bucket/data/_staging",
        ],
    )
    assert (result.filtering, result.pattern_field) == ("by_rule", "path_specs")
    assert verdicts == {
        "gs://my-bucket/data/events": (True, None),
        "gs://my-bucket/data/users": (False, "path_specs[0].tables_filter_pattern"),
        "gs://my-bucket/data/_staging": (
            False,
            "path_specs[0].include_hidden_folders",
        ),
    }
    assert {r.target for r in result.results} == {
        "gs://my-bucket/data/events",
        "gs://my-bucket/data/users",
        "gs://my-bucket/data/_staging",
    }


def test_simple_spec_files_use_the_ingestion_rule() -> None:
    _, verdicts = _verdicts(
        _recipe("gs://my-bucket/raw/*.csv", exclude=["**/tmp_*.csv"]),
        "Table",
        [
            "gs://my-bucket/raw/a.csv",
            "gs://my-bucket/raw/tmp_b.csv",
            "gs://my-bucket/raw/notes.txt",
        ],
    )
    assert verdicts == {
        "gs://my-bucket/raw/a.csv": (True, None),
        "gs://my-bucket/raw/tmp_b.csv": (False, "path_specs[0].exclude"),
        "gs://my-bucket/raw/notes.txt": (False, "path_specs[0].include"),
    }


def test_gs_excludes_are_rewritten_like_the_include() -> None:
    _, verdicts = _verdicts(
        _recipe(
            "gs://my-bucket/raw/*.csv", exclude=["gs://my-bucket/raw/skip_*.csv"]
        ),
        "Table",
        ["gs://my-bucket/raw/skip_1.csv"],
    )
    assert verdicts["gs://my-bucket/raw/skip_1.csv"] == (
        False,
        "path_specs[0].exclude",
    )


def test_autodetected_partition_suffix_is_judged_like_ingestion() -> None:
    # An include ending in {table} gets "/**" appended by PathSpec, which
    # equivalent_s3_path_specs strips and re-adds; the verdict must survive it.
    _, verdicts = _verdicts(
        _recipe("gs://my-bucket/data/{table}"),
        "Table",
        ["gs://my-bucket/data/events"],
    )
    assert verdicts["gs://my-bucket/data/events"] == (True, None)


def test_any_including_spec_wins() -> None:
    recipe: Dict[str, object] = {
        "credential": HMAC,
        "path_specs": [
            {
                "include": "gs://my-bucket/data/{table}/*.parquet",
                "tables_filter_pattern": {"deny": [".*"]},
            },
            {"include": "gs://my-bucket/data/{table}/*.parquet"},
        ],
    }
    _, verdicts = _verdicts(recipe, "Table", ["gs://my-bucket/data/events"])
    assert verdicts["gs://my-bucket/data/events"] == (True, None)


@pytest.mark.parametrize("kind", ["Table", "Folder"])
def test_a_non_gs_name_is_a_caller_error(kind: str) -> None:
    with pytest.raises(ValueError, match="gs://"):
        _verdicts(
            _recipe("gs://my-bucket/data/*.csv"), kind, ["s3://my-bucket/a.csv"]
        )


def test_a_bucket_name_with_a_slash_is_a_caller_error() -> None:
    with pytest.raises(ValueError, match="bucket name"):
        _verdicts(
            _recipe("gs://my-bucket/data/*.csv"), "GCS bucket", ["gs://my-bucket"]
        )


def test_buckets_and_folders_are_rule_filtered_too() -> None:
    recipe = _recipe("gs://my-bucket/media/*/", emit_folders_only=True)
    result, buckets = _verdicts(recipe, "GCS bucket", ["my-bucket", "other"])
    assert result.filtering == "by_rule"
    assert buckets == {"my-bucket": (True, None), "other": (False, "path_specs")}
    _, folders = _verdicts(
        recipe,
        "Folder",
        ["gs://my-bucket/media/photos", "gs://my-bucket/media/.cache"],
    )
    assert folders == {
        "gs://my-bucket/media/photos": (True, None),
        "gs://my-bucket/media/.cache": (
            False,
            "path_specs[0].include_hidden_folders",
        ),
    }


def test_a_folder_above_datasets_is_a_container() -> None:
    result, folders = _verdicts(
        _recipe("gs://my-bucket/data/{table}/*.parquet"),
        "Folder",
        ["gs://my-bucket/data", "gs://my-bucket/other"],
    )
    assert folders == {
        "gs://my-bucket/data": (True, None),
        "gs://my-bucket/other": (False, "path_specs"),
    }
    assert result.warnings

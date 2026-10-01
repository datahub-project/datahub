from typing import Dict, List, Optional, Tuple

import pytest

from datahub.ingestion.agent.filter_check import FilterCheckResult, check_filters

AWS: Dict[str, object] = {
    "aws_access_key_id": "id",
    "aws_secret_access_key": "secret",
    "aws_region": "us-east-1",
}


def _recipe(
    *specs: Dict[str, object],
    aws: Optional[Dict[str, object]] = None,
    **extra: object,
) -> Dict[str, object]:
    recipe: Dict[str, object] = {"path_specs": list(specs), **extra}
    if aws is not None:
        recipe["aws_config"] = aws
    return recipe


def _verdicts(
    recipe: Dict[str, object], kind: str, names: List[str]
) -> Tuple[FilterCheckResult, Dict[str, Tuple[bool, Optional[str]]]]:
    result = check_filters(
        source_type="s3", config_dict=recipe, kind=kind, parent_path=[], names=names
    )
    return result, {r.name: (r.included, r.excluded_by) for r in result.results}


def test_tables_are_judged_by_path_specs() -> None:
    result, verdicts = _verdicts(
        _recipe(
            {
                "include": "s3://my-bucket/data/{table}/*/*.csv",
                "tables_filter_pattern": {"deny": ["^users$"]},
            },
            aws=AWS,
        ),
        "Table",
        [
            "s3://my-bucket/data/events",
            "s3://my-bucket/data/users",
            "s3://my-bucket/data/_tmp",
        ],
    )
    assert (result.filtering, result.pattern_field) == ("by_rule", "path_specs")
    assert verdicts == {
        "s3://my-bucket/data/events": (True, None),
        "s3://my-bucket/data/users": (False, "path_specs[0].tables_filter_pattern"),
        "s3://my-bucket/data/_tmp": (False, "path_specs[0].include_hidden_folders"),
    }


def test_content_type_mode_includes_extensionless_files() -> None:
    spec: Dict[str, object] = {"include": "s3://my-bucket/no_ext/*"}
    _, off = _verdicts(_recipe(spec, aws=AWS), "Table", ["s3://my-bucket/no_ext/blob"])
    _, on = _verdicts(
        _recipe(spec, aws=AWS, use_s3_content_type=True),
        "Table",
        ["s3://my-bucket/no_ext/blob"],
    )
    assert off["s3://my-bucket/no_ext/blob"] == (
        False,
        "path_specs[0].default_extension",
    )
    assert on["s3://my-bucket/no_ext/blob"] == (True, None)


def test_buckets_follow_a_bucket_wildcard_include() -> None:
    _, verdicts = _verdicts(
        _recipe({"include": "s3://my-bucket*/data/{table}/*.csv"}, aws=AWS),
        "S3 bucket",
        ["my-bucket", "my-bucket-2", "other"],
    )
    assert verdicts == {
        "my-bucket": (True, None),
        "my-bucket-2": (True, None),
        "other": (False, "path_specs"),
    }


def test_folders_only_specs_judge_folders() -> None:
    _, verdicts = _verdicts(
        _recipe(
            {"include": "s3://my-bucket/media/*/", "emit_folders_only": True}, aws=AWS
        ),
        "Folder",
        ["s3://my-bucket/media/photos", "s3://my-bucket/media/.cache"],
    )
    assert verdicts["s3://my-bucket/media/photos"] == (True, None)
    assert verdicts["s3://my-bucket/media/.cache"] == (
        False,
        "path_specs[0].include_hidden_folders",
    )


@pytest.mark.parametrize(
    "name", ["my-bucket/data/events", "s3a://my-bucket/data/events"]
)
def test_a_name_that_is_not_an_s3_uri_is_a_caller_error(name: str) -> None:
    with pytest.raises(ValueError, match="s3://"):
        _verdicts(
            _recipe({"include": "s3://my-bucket/data/*.csv"}, aws=AWS), "Table", [name]
        )


def test_a_bucket_name_with_a_slash_is_a_caller_error() -> None:
    with pytest.raises(ValueError, match="bucket name"):
        _verdicts(
            _recipe({"include": "s3://my-bucket/data/*.csv"}, aws=AWS),
            "S3 bucket",
            ["my-bucket/data"],
        )


def test_a_local_recipe_is_refused() -> None:
    with pytest.raises(ValueError, match="s3://"):
        _verdicts(
            _recipe({"include": "/data/{table}/*.csv"}), "Table", ["/data/events"]
        )


def test_a_recipe_without_aws_config_is_judged_with_a_warning() -> None:
    result, verdicts = _verdicts(
        _recipe({"include": "s3://my-bucket/data/*.csv"}),
        "Table",
        ["s3://my-bucket/data/a.csv"],
    )
    assert verdicts["s3://my-bucket/data/a.csv"] == (True, None)
    assert any("aws_config" in w for w in result.warnings)

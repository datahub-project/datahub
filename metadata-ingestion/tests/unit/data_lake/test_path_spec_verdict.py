from typing import Callable, List, Sequence, Tuple

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.agent.verdicts import Verdict
from datahub.ingestion.source.data_lake_common.path_spec import PathSpec
from datahub.ingestion.source.data_lake_common.path_spec_verdict import (
    TEMPLATED_FILE_RULES_WARNING,
    UNPARSED_TABLE_WARNING,
    judge_bucket,
    judge_dataset,
    judge_folder,
)

TEMPLATED = PathSpec(
    include="s3://my-bucket/data/{table}/*.parquet",
    tables_filter_pattern=AllowDenyPattern(deny=["^users$"]),
)
SIMPLE = PathSpec(include="s3://my-bucket/raw/*.csv", exclude=["**/tmp_*.csv"])
FOLDERS = PathSpec(include="s3://my-bucket/media/*/", emit_folders_only=True)

Judge = Callable[[Sequence[PathSpec], str, Callable[[str], None]], Verdict]


def _judge(
    fn: Judge, specs: Sequence[PathSpec], name: str
) -> Tuple[Verdict, List[str]]:
    warnings: List[str] = []
    return fn(specs, name, warnings.append), warnings


def test_templated_table_folder_is_judged_by_tables_filter_pattern() -> None:
    v, _ = _judge(judge_dataset, [TEMPLATED], "s3://my-bucket/data/events")
    assert v.included
    v, _ = _judge(judge_dataset, [TEMPLATED], "s3://my-bucket/data/users")
    assert (v.included, v.excluded_by) == (
        False,
        "path_specs[0].tables_filter_pattern",
    )


def test_hidden_table_folder_is_excluded() -> None:
    v, _ = _judge(judge_dataset, [TEMPLATED], "s3://my-bucket/data/_staging")
    assert v.excluded_by == "path_specs[0].include_hidden_folders"


def test_templated_verdict_warns_that_file_rules_are_not_judged() -> None:
    _, warnings = _judge(judge_dataset, [TEMPLATED], "s3://my-bucket/data/events")
    assert warnings == [TEMPLATED_FILE_RULES_WARNING]


def test_simple_spec_file_uses_the_exact_ingestion_rule() -> None:
    v, _ = _judge(judge_dataset, [SIMPLE], "s3://my-bucket/raw/tmp_1.csv")
    assert v.excluded_by == "path_specs[0].exclude"
    v, _ = _judge(judge_dataset, [SIMPLE], "s3://my-bucket/raw/a.json")
    assert v.excluded_by == "path_specs[0].include"


def test_a_name_no_spec_reaches_is_excluded_by_path_specs() -> None:
    v, _ = _judge(judge_dataset, [TEMPLATED, SIMPLE], "s3://other-bucket/x.csv")
    assert (v.included, v.excluded_by) == (False, "path_specs")


def test_any_including_spec_wins() -> None:
    denying = PathSpec(
        include="s3://my-bucket/data/{table}/*.parquet",
        tables_filter_pattern=AllowDenyPattern(deny=[".*"]),
    )
    v, _ = _judge(judge_dataset, [denying, TEMPLATED], "s3://my-bucket/data/events")
    assert v.included


def test_folders_only_spec_uses_folder_rules() -> None:
    v, _ = _judge(judge_folder, [FOLDERS], "s3://my-bucket/media/.cache")
    assert v.excluded_by == "path_specs[0].include_hidden_folders"
    v, _ = _judge(judge_folder, [FOLDERS], "s3://my-bucket/media/photos")
    assert v.included


def test_a_folder_above_datasets_is_included_as_a_parent_with_a_warning() -> None:
    v, warnings = _judge(judge_folder, [TEMPLATED], "s3://my-bucket/data")
    assert v.included and warnings


def test_bucket_is_included_when_a_spec_reaches_it() -> None:
    wildcard = PathSpec(include="s3://my-*/data/{table}/*.parquet")
    assert _judge(judge_bucket, [wildcard], "my-bucket")[0].included
    assert _judge(judge_bucket, [wildcard], "other")[0].excluded_by == "path_specs"


def test_a_dot_table_folder_fails_the_include_even_when_hidden_folders_are_on() -> None:
    # list_folders_path returns `.staging`, but allowed() globs each file under it
    # without matching a leading dot, so ingestion emits nothing for it.
    spec = PathSpec(
        include="s3://my-bucket/data/{table}/*.parquet", include_hidden_folders=True
    )
    v, _ = _judge(judge_dataset, [spec], "s3://my-bucket/data/.staging")
    assert v.excluded_by == "path_specs[0].include"
    v, _ = _judge(judge_dataset, [spec], "s3://my-bucket/data/_staging")
    assert v.included


def test_content_type_mode_skips_the_extension_rules_like_ingestion() -> None:
    spec = PathSpec(include="s3://my-bucket/no_ext/*")
    v, _ = _judge(judge_dataset, [spec], "s3://my-bucket/no_ext/blob")
    assert (v.included, v.excluded_by) == (False, "path_specs[0].default_extension")
    warnings: List[str] = []
    v = judge_dataset(
        [spec], "s3://my-bucket/no_ext/blob", warnings.append, ignore_ext=True
    )
    assert v.included


def test_a_table_folder_ingestion_cannot_name_is_included_with_a_warning() -> None:
    # `my-bucket*` globs `my-bucket`, but parse() needs a character for the
    # wildcard, so ingestion names that table after a file inside it.
    spec = PathSpec(include="s3://my-bucket*/data/{table}/*.parquet")
    v, warnings = _judge(judge_dataset, [spec], "s3://my-bucket/data/events")
    assert v.included and UNPARSED_TABLE_WARNING in warnings
    v, warnings = _judge(judge_dataset, [spec], "s3://my-bucket-2/data/events")
    assert v.included and UNPARSED_TABLE_WARNING not in warnings

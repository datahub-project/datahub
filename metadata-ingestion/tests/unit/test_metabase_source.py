from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.metabase import (
    MetabaseConfig,
    MetabaseReport,
    MetabaseSource,
)


class TestMetabaseSource(MetabaseSource):
    def __init__(self, ctx: PipelineContext, config: MetabaseConfig):
        self.config = config
        self.report = MetabaseReport()


def test_get_platform_instance():
    ctx = PipelineContext(run_id="test-metabase")
    config = MetabaseConfig()
    config.connect_uri = "http://localhost:3000"
    # config.database_id_to_instance_map = {"42": "my_main_clickhouse"}
    # config.platform_instance_map = {"clickhouse": "my_only_clickhouse"}
    metabase = TestMetabaseSource(ctx, config)

    # no mappings defined
    assert metabase.get_platform_instance("clickhouse", 42) is None

    # database_id_to_instance_map is defined, key is present
    metabase.config.database_id_to_instance_map = {"42": "my_main_clickhouse"}
    assert metabase.get_platform_instance(None, 42) == "my_main_clickhouse"

    # database_id_to_instance_map is defined, key is missing
    assert metabase.get_platform_instance(None, 999) is None

    # database_id_to_instance_map is defined, key is missing, platform_instance_map is defined and key present
    metabase.config.platform_instance_map = {"clickhouse": "my_only_clickhouse"}
    assert metabase.get_platform_instance("clickhouse", 999) == "my_only_clickhouse"

    # database_id_to_instance_map is defined, key is missing, platform_instance_map is defined and key missing
    assert metabase.get_platform_instance("missing-platform", 999) is None

    # database_id_to_instance_map is missing, platform_instance_map is defined and key present
    metabase.config.database_id_to_instance_map = None
    assert metabase.get_platform_instance("clickhouse", 999) == "my_only_clickhouse"

    # database_id_to_instance_map is missing, platform_instance_map is defined and key missing
    assert metabase.get_platform_instance("missing-platform", 999) is None


def test_set_display_uri():
    display_uri = "some_host:1234"

    config = MetabaseConfig.parse_obj({"display_uri": display_uri})

    assert config.connect_uri == "localhost:3000"
    assert config.display_uri == display_uri

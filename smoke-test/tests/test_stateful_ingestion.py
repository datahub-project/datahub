from typing import Any, Dict, Optional, cast

from sqlalchemy import create_engine
from sqlalchemy.sql import text

from datahub.ingestion.api.committable import StatefulCommittable
from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.sql.mysql import MySQLConfig, MySQLSource
from datahub.ingestion.source.state.entity_removal_state import GenericCheckpointState
from datahub.ingestion.source.state.checkpoint import Checkpoint
from tests.utils import (
    get_gms_url,
    get_mysql_password,
    get_mysql_url,
    get_mysql_username,
)


def test_stateful_ingestion(wait_for_healthchecks):
    def create_mysql_engine(mysql_source_config_dict: Dict[str, Any]) -> Any:
        mysql_config = MySQLConfig.parse_obj(mysql_source_config_dict)
        url = mysql_config.get_sql_alchemy_url()
        return create_engine(url)

    def create_table(engine: Any, name: str, defn: str) -> None:
        create_table_query = text(f"CREATE TABLE IF NOT EXISTS {name}{defn};")
        engine.execute(create_table_query)

    def drop_table(engine: Any, table_name: str) -> None:
        drop_table_query = text(f"DROP TABLE {table_name};")
        engine.execute(drop_table_query)

    def run_and_get_pipeline(pipeline_config_dict: Dict[str, Any]) -> Pipeline:
        pipeline = Pipeline.create(pipeline_config_dict)
        pipeline.run()
        pipeline.raise_from_status()
        return pipeline

    def validate_all_providers_have_committed_successfully(pipeline: Pipeline) -> None:
        provider_count: int = 0
        for name, provider in pipeline.ctx.get_committables():
            provider_count += 1
            assert isinstance(provider, StatefulCommittable)
            stateful_committable = cast(StatefulCommittable, provider)
            assert stateful_committable.has_successfully_committed()
            assert stateful_committable.state_to_commit
        assert provider_count == 1

    def get_current_checkpoint_from_pipeline(
        pipeline: Pipeline,
    ) -> Optional[Checkpoint[GenericCheckpointState]]:
        mysql_source = cast(MySQLSource, pipeline.source)
        return mysql_source.get_current_checkpoint(
            mysql_source.stale_entity_removal_handler.job_id
        )

    source_config_dict: Dict[str, Any] = {
        "host_port": get_mysql_url(),
        "username": get_mysql_username(),
        "password": get_mysql_password(),
        "database": "datahub",
        "stateful_ingestion": {
            "enabled": True,
            "remove_stale_metadata": True,
            "fail_safe_threshold": 100.0,
            "state_provider": {
                "type": "datahub",
                "config": {"datahub_api": {"server": get_gms_url()}},
            },
        },
    }

    pipeline_config_dict: Dict[str, Any] = {
        "source": {
            "type": "mysql",
            "config": source_config_dict,
        },
        "sink": {
            "type": "datahub-rest",
            "config": {"server": get_gms_url()},
        },
        "pipeline_name": "mysql_stateful_ingestion_smoke_test_pipeline",
        "reporting": [
            {
                "type": "datahub",
            }
        ],
    }

    # 1. Setup the SQL engine
    mysql_engine = create_mysql_engine(source_config_dict)

    # 2. Create test tables for first run of the  pipeline.
    table_prefix = "stateful_ingestion_test"
    table_defs = {
        f"{table_prefix}_t1": "(id INT, name VARCHAR(10))",
        f"{table_prefix}_t2": "(id INT)",
    }
    table_names = sorted(table_defs.keys())
    for table_name, defn in table_defs.items():
        create_table(mysql_engine, table_name, defn)

    # 3. Do the first run of the pipeline and get the default job's checkpoint.
    pipeline_run1 = run_and_get_pipeline(pipeline_config_dict)
    checkpoint1 = get_current_checkpoint_from_pipeline(pipeline_run1)
    assert checkpoint1
    assert checkpoint1.state

    # 4. Drop table t1 created during step 2 + rerun the pipeline and get the checkpoint state.
    drop_table(mysql_engine, table_names[0])
    pipeline_run2 = run_and_get_pipeline(pipeline_config_dict)
    checkpoint2 = get_current_checkpoint_from_pipeline(pipeline_run2)
    assert checkpoint2
    assert checkpoint2.state

    # 5. Perform all assertions on the states
    state1 = checkpoint1.state
    state2 = checkpoint2.state
    difference_urns = list(
        state1.get_urns_not_in(type="*", other_checkpoint_state=state2)
    )
    assert len(difference_urns) == 1
    assert (
        difference_urns[0]
        == "urn:li:dataset:(urn:li:dataPlatform:mysql,datahub.stateful_ingestion_test_t1,PROD)"
    )

    # 6. Cleanup table t2 as well to prevent other tests that rely on data in the smoke-test world.
    drop_table(mysql_engine, table_names[1])

    # 7. Validate that all providers have committed successfully.
    # NOTE: The following validation asserts for presence of state as well
    # and validates reporting.
    validate_all_providers_have_committed_successfully(pipeline_run1)
    validate_all_providers_have_committed_successfully(pipeline_run2)

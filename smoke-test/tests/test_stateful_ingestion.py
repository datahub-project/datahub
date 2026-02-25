from typing import Any, Dict, Union, cast

from sqlalchemy import create_engine
from sqlalchemy.sql import text

from datahub.ingestion.api.committable import StatefulCommittable
from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.sql.mysql import MySQLConfig
from datahub.ingestion.source.sql.postgres import PostgresConfig
from datahub.testing.state_helpers import get_current_checkpoint_from_pipeline
from tests.utils import get_db_password, get_db_type, get_db_url, get_db_username


def test_stateful_ingestion(auth_session):
    def create_db_engine(sql_source_config_dict: Dict[str, Any]) -> Any:
        sql_config: Union[MySQLConfig, PostgresConfig]
        if get_db_type() == "mysql":
            sql_config = MySQLConfig.model_validate(sql_source_config_dict)
        else:
            sql_config = PostgresConfig.model_validate(sql_source_config_dict)

        url = sql_config.get_sql_alchemy_url()
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
        for _, provider in pipeline.ctx.get_committables():
            provider_count += 1
            assert isinstance(provider, StatefulCommittable)
            stateful_committable = cast(StatefulCommittable, provider)
            assert stateful_committable.has_successfully_committed()
            assert stateful_committable.state_to_commit
        assert provider_count == 1

    source_config_dict: Dict[str, Any] = {
        "host_port": get_db_url(),
        "username": get_db_username(),
        "password": get_db_password(),
        "database": "datahub",
        "stateful_ingestion": {
            "enabled": True,
            "remove_stale_metadata": True,
            "fail_safe_threshold": 100.0,
        },
    }

    pipeline_config_dict: Dict[str, Any] = {
        "source": {
            "type": get_db_type(),
            "config": source_config_dict,
        },
        "sink": {
            "type": "datahub-rest",
            "config": {
                "server": auth_session.gms_url(),
                "token": auth_session.gms_token(),
            },
        },
        "pipeline_name": "sql_stateful_ingestion_smoke_test_pipeline",
        "reporting": [
            {
                "type": "datahub",
            }
        ],
    }

    # 1. Setup the SQL engine
    sql_engine = create_db_engine(source_config_dict)

    # 2. Create test tables for first run of the  pipeline.
    table_prefix = "stateful_ingestion_test"
    table_defs = {
        f"{table_prefix}_t1": "(id INT, name VARCHAR(10))",
        f"{table_prefix}_t2": "(id INT)",
    }
    table_names = sorted(table_defs.keys())
    for table_name, defn in table_defs.items():
        create_table(sql_engine, table_name, defn)

    # 3. Do the first run of the pipeline and get the default job's checkpoint.
    pipeline_run1 = run_and_get_pipeline(pipeline_config_dict)
    checkpoint1 = get_current_checkpoint_from_pipeline(pipeline_run1)
    assert checkpoint1
    assert checkpoint1.state

    # 4. Drop table t1 created during step 2 + rerun the pipeline and get the checkpoint state.
    drop_table(sql_engine, table_names[0])
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
    table_prefix = "datahub" if (get_db_type() == "mysql") else "datahub.public"
    assert (
        difference_urns[0]
        == f"urn:li:dataset:(urn:li:dataPlatform:{get_db_type()},{table_prefix}.stateful_ingestion_test_t1,PROD)"
    )

    # 6. Cleanup table t2 as well to prevent other tests that rely on data in the smoke-test world.
    drop_table(sql_engine, table_names[1])

    # 7. Validate that all providers have committed successfully.
    # NOTE: The following validation asserts for presence of state as well
    # and validates reporting.
    validate_all_providers_have_committed_successfully(pipeline_run1)
    validate_all_providers_have_committed_successfully(pipeline_run2)

from datahub.ingestion.source.qlik_sense.config import QlikSourceConfig


def test_stateful_ingestion_supports_stale_removal_config():
    config = QlikSourceConfig.model_validate(
        {
            "tenant_hostname": "tenant.example",
            "api_key": "secret",
            "stateful_ingestion": {
                "enabled": True,
                "remove_stale_metadata": True,
            },
        }
    )

    assert config.stateful_ingestion is not None
    assert config.stateful_ingestion.remove_stale_metadata is True

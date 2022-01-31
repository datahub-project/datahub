from typing import Any, Dict, Optional, Tuple, Type, cast

import pytest
from pydantic import ValidationError

from datahub.configuration.common import ConfigModel, DynamicTypedConfig
from datahub.ingestion.graph.client import DatahubClientConfig
from datahub.ingestion.reporting.datahub_ingestion_reporting_provider import (
    DatahubIngestionReportingProviderConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfig,
)
from datahub.ingestion.source.state_provider.datahub_ingestion_checkpointing_provider import (
    DatahubIngestionStateProviderConfig,
)

# 0. Common client configs.
datahub_client_configs: Dict[str, Any] = {
    "full": {
        "server": "http://localhost:8080",
        "token": "dummy_test_tok",
        "timeout_sec": 10,
        "extra_headers": {},
        "max_threads": 1,
    },
    "simple": {},
    "default": {},
    "none": None,
}


# 1. Datahub Checkpointing State Provider Config test params
checkpointing_provider_config_test_params: Dict[
    str,
    Tuple[
        Type[DatahubIngestionStateProviderConfig],
        Dict[str, Any],
        Optional[DatahubIngestionStateProviderConfig],
        bool,
    ],
] = {
    # Full custom-config
    "checkpointing_valid_full_config": (
        DatahubIngestionStateProviderConfig,
        {
            "datahub_api": datahub_client_configs["full"],
        },
        DatahubIngestionStateProviderConfig(
            datahub_api=DatahubClientConfig(
                server="http://localhost:8080",
                token="dummy_test_tok",
                timeout_sec=10,
                extra_headers={},
                max_threads=1,
            ),
        ),
        False,
    ),
    # Simple config
    "checkpointing_valid_simple_config": (
        DatahubIngestionStateProviderConfig,
        {
            "datahub_api": datahub_client_configs["simple"],
        },
        DatahubIngestionStateProviderConfig(
            datahub_api=DatahubClientConfig(
                server="http://localhost:8080",
            ),
        ),
        False,
    ),
    # Default
    "checkpointing_default": (
        DatahubIngestionStateProviderConfig,
        {
            "datahub_api": datahub_client_configs["default"],
        },
        DatahubIngestionStateProviderConfig(
            datahub_api=DatahubClientConfig(),
        ),
        False,
    ),
    # None
    "checkpointing_bad_config": (
        DatahubIngestionStateProviderConfig,
        datahub_client_configs["none"],
        None,
        True,
    ),
}

# 2. Datahub Reporting Provider Config test params
reporting_provider_config_test_params: Dict[
    str,
    Tuple[
        Type[DatahubIngestionReportingProviderConfig],
        Dict[str, Any],
        Optional[DatahubIngestionReportingProviderConfig],
        bool,
    ],
] = {
    # Full custom-config
    "reporting_valid_full_config": (
        DatahubIngestionReportingProviderConfig,
        {
            "datahub_api": datahub_client_configs["full"],
        },
        DatahubIngestionReportingProviderConfig(
            datahub_api=DatahubClientConfig(
                server="http://localhost:8080",
                token="dummy_test_tok",
                timeout_sec=10,
                extra_headers={},
                max_threads=1,
            ),
        ),
        False,
    ),
    # Simple config
    "reporting_valid_simple_config": (
        DatahubIngestionReportingProviderConfig,
        {
            "datahub_api": datahub_client_configs["simple"],
        },
        DatahubIngestionReportingProviderConfig(
            datahub_api=DatahubClientConfig(
                server="http://localhost:8080",
            ),
        ),
        False,
    ),
    # Default
    "reporting_default": (
        DatahubIngestionReportingProviderConfig,
        {
            "datahub_api": datahub_client_configs["default"],
        },
        DatahubIngestionReportingProviderConfig(
            datahub_api=DatahubClientConfig(),
        ),
        False,
    ),
    # None
    "reporting_bad_config": (
        DatahubIngestionReportingProviderConfig,
        datahub_client_configs["none"],
        None,
        True,
    ),
}


# 3. StatefulIngestion Config test params
stateful_ingestion_config_test_params: Dict[
    str,
    Tuple[
        Type[StatefulIngestionConfig],
        Dict[str, Any],
        Optional[StatefulIngestionConfig],
        bool,
    ],
] = {
    # Ful custom-config
    "stateful_ingestion_full_custom": (
        StatefulIngestionConfig,
        {
            "enabled": True,
            "max_checkpoint_state_size": 1024,
            "state_provider": {
                "type": "datahub",
                "config": datahub_client_configs["full"],
            },
            "ignore_old_state": True,
            "ignore_new_state": True,
        },
        StatefulIngestionConfig(
            enabled=True,
            max_checkpoint_state_size=1024,
            ignore_old_state=True,
            ignore_new_state=True,
            state_provider=DynamicTypedConfig(
                type="datahub",
                config=datahub_client_configs["full"],
            ),
        ),
        False,
    ),
    # Default disabled
    "stateful_ingestion_default_disabled": (
        StatefulIngestionConfig,
        {},
        StatefulIngestionConfig(
            enabled=False,
            # fmt: off
            max_checkpoint_state_size=2**24,
            # fmt: on
            ignore_old_state=False,
            ignore_new_state=False,
            state_provider=None,
        ),
        False,
    ),
    # Default enabled
    "stateful_ingestion_default_enabled": (
        StatefulIngestionConfig,
        {"enabled": True},
        StatefulIngestionConfig(
            enabled=True,
            # fmt: off
            max_checkpoint_state_size=2**24,
            # fmt: on
            ignore_old_state=False,
            ignore_new_state=False,
            state_provider=DynamicTypedConfig(type="datahub", config=None),
        ),
        False,
    ),
    # Bad Config- throws ValidationError
    "stateful_ingestion_bad_config": (
        StatefulIngestionConfig,
        {"enabled": True, "state_provider": {}},
        None,
        True,
    ),
}

# 4. Combine all of the config params from 1, 2 & 3 above for the common parametrized test.
CombinedTestConfigType = Dict[
    str,
    Tuple[
        Type[ConfigModel],
        Dict[str, Any],
        Optional[ConfigModel],
        bool,
    ],
]

combined_test_configs = {
    **cast(CombinedTestConfigType, checkpointing_provider_config_test_params),
    **cast(CombinedTestConfigType, reporting_provider_config_test_params),
    **cast(CombinedTestConfigType, stateful_ingestion_config_test_params),
}


@pytest.mark.parametrize(
    "config_class, config_dict, expected, raises_exception",
    combined_test_configs.values(),
    ids=combined_test_configs.keys(),
)
def test_state_provider_configs(
    config_class: Type[ConfigModel],
    config_dict: Dict[str, Any],
    expected: Optional[ConfigModel],
    raises_exception: bool,
) -> None:
    if raises_exception:
        with pytest.raises(ValidationError):
            assert expected is None
            config_class.parse_obj(config_dict)
    else:
        config = config_class.parse_obj(config_dict)
        assert config == expected

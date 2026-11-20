"""Clients that never use custom environments keep the Python SDK API exactly as before.

Env / origin fields became a string typeref (FabricTypeName), so their generated hints
are now ``str``. FabricTypeClass is still generated with every symbol, its values are
still strings, and code that passes FabricTypeClass members keeps working unchanged.
"""

from typing import Callable, List, Tuple

import pytest

from datahub.configuration.source_common import EnvConfigMixin
from datahub.emitter.mce_builder import (
    ALL_ENV_TYPES,
    DEFAULT_ENV,
    make_dataset_urn,
    make_dataset_urn_with_platform_instance,
    make_ml_model_group_urn,
    make_ml_model_urn,
)
from datahub.metadata.schema_classes import (
    ContainerPropertiesClass,
    DataFlowInfoClass,
    DataJobInfoClass,
    DataProcessKeyClass,
    DatasetKeyClass,
    FabricTypeClass,
    IcebergWarehouseInfoClass,
    MLModelDeploymentKeyClass,
    MLModelGroupKeyClass,
    MLModelKeyClass,
)
from datahub.metadata.urns import DatasetUrn

BUILT_INS: List[str] = sorted(ALL_ENV_TYPES)

# Each adopting aspect, built the old way with a FabricTypeClass member.
ASPECTS: List[Tuple[str, Callable[[str], object], str]] = [
    (
        "DatasetKey",
        lambda env: DatasetKeyClass(
            platform="urn:li:dataPlatform:hive", name="t", origin=env
        ),
        "origin",
    ),
    (
        "MLModelKey",
        lambda env: MLModelKeyClass(
            platform="urn:li:dataPlatform:mlflow", name="m", origin=env
        ),
        "origin",
    ),
    (
        "MLModelGroupKey",
        lambda env: MLModelGroupKeyClass(
            platform="urn:li:dataPlatform:mlflow", name="g", origin=env
        ),
        "origin",
    ),
    (
        "MLModelDeploymentKey",
        lambda env: MLModelDeploymentKeyClass(
            platform="urn:li:dataPlatform:mlflow", name="d", origin=env
        ),
        "origin",
    ),
    (
        "DataProcessKey",
        lambda env: DataProcessKeyClass(orchestrator="airflow", name="p", origin=env),
        "origin",
    ),
    (
        "ContainerProperties",
        lambda env: ContainerPropertiesClass(name="c", env=env),
        "env",
    ),
    (
        "DataFlowInfo",
        lambda env: DataFlowInfoClass(name="f", env=env),
        "env",
    ),
    (
        "DataJobInfo",
        lambda env: DataJobInfoClass(name="j", type="COMMAND", env=env),
        "env",
    ),
    (
        "IcebergWarehouseInfo",
        lambda env: IcebergWarehouseInfoClass(
            dataRoot="s3://b/wh",
            clientId="urn:li:dataHubSecret:id",
            clientSecret="urn:li:dataHubSecret:secret",
            region="us-east-1",
            env=env,
        ),
        "env",
    ),
]


def test_fabric_type_class_is_unchanged() -> None:
    symbols = {
        name: value
        for name, value in vars(FabricTypeClass).items()
        if not name.startswith("_") and isinstance(value, str)
    }
    assert len(symbols) == 17
    assert all(name == value for name, value in symbols.items())
    assert set(symbols.values()) == ALL_ENV_TYPES
    assert DEFAULT_ENV == FabricTypeClass.PROD == "PROD"


@pytest.mark.parametrize("name,build,field", ASPECTS, ids=[a[0] for a in ASPECTS])
def test_aspects_accept_fabric_type_class_members(
    name: str, build: Callable[[str], object], field: str
) -> None:
    for env in BUILT_INS:
        aspect = build(getattr(FabricTypeClass, env))
        assert aspect.validate()  # type: ignore[attr-defined]
        obj = aspect.to_obj()  # type: ignore[attr-defined]
        assert obj[field] == env
        copy = type(aspect).from_obj(obj)  # type: ignore[attr-defined]
        assert getattr(copy, field) == getattr(FabricTypeClass, env)


def test_urn_builders_are_unchanged() -> None:
    assert (
        make_dataset_urn("hive", "db.t", FabricTypeClass.DEV)
        == "urn:li:dataset:(urn:li:dataPlatform:hive,db.t,DEV)"
    )
    assert (
        make_dataset_urn("hive", "db.t")
        == "urn:li:dataset:(urn:li:dataPlatform:hive,db.t,PROD)"
    )
    assert (
        make_dataset_urn_with_platform_instance(
            "hive", "db.t", "inst", FabricTypeClass.QA
        )
        == "urn:li:dataset:(urn:li:dataPlatform:hive,inst.db.t,QA)"
    )
    assert (
        make_ml_model_urn("mlflow", "m", FabricTypeClass.PROD)
        == "urn:li:mlModel:(urn:li:dataPlatform:mlflow,m,PROD)"
    )
    assert (
        make_ml_model_group_urn("mlflow", "g", FabricTypeClass.PROD)
        == "urn:li:mlModelGroup:(urn:li:dataPlatform:mlflow,g,PROD)"
    )

    urn = DatasetUrn.from_string("urn:li:dataset:(urn:li:dataPlatform:hive,db.t,PROD)")
    assert urn.env == FabricTypeClass.PROD
    assert DatasetUrn("hive", "db.t", FabricTypeClass.PROD) == urn


@pytest.mark.parametrize("env", BUILT_INS)
def test_recipe_env_accepts_every_built_in(env: str) -> None:
    assert EnvConfigMixin.model_validate({"env": env}).env == env
    assert EnvConfigMixin.model_validate({"env": env.lower()}).env == env

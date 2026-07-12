from typing import Optional

import pydantic
from pydantic import SecretStr

from datahub.ingestion.autogen_ui.inference import build_form


class AwsLikeConfig(pydantic.BaseModel):
    aws_access_key_id: str = pydantic.Field(description="Access key ID.")
    aws_secret_access_key: SecretStr = pydantic.Field(description="Secret access key.")


class HostConfig(pydantic.BaseModel):
    host: str = pydantic.Field(description="Host name.")
    aws_config: Optional[AwsLikeConfig] = pydantic.Field(
        default=None, description="AWS credentials."
    )


def test_nested_secret_container_renders_as_group_and_masks_child() -> None:
    form = build_form("s3like", "s3like", HostConfig)
    connection = next(s for s in form.sections if s.key == "connection")

    # The secret-bearing sub-model becomes a collapsible group in Connection.
    group = next(f for f in connection.fields if f.name == "aws_config")
    assert group.widget == "group"
    assert group.group_fields is not None

    secret_field = next(
        f for f in group.group_fields if f.name == "aws_secret_access_key"
    )
    assert secret_field.secret is True
    assert secret_field.widget == "password"
    assert secret_field.field_path == "source.config.aws_config.aws_secret_access_key"

    # It is not rendered as an opaque object blob at the top level.
    assert group.widget != "keyvalue"


class RequiredSubModel(pydantic.BaseModel):
    aws_access_key_id: str = pydantic.Field(description="Access key ID.")
    aws_secret_access_key: SecretStr = pydantic.Field(description="Secret access key.")


class OptionalContainerConfig(pydantic.BaseModel):
    host: str = pydantic.Field(description="Host name.")
    aws_config: Optional[RequiredSubModel] = pydantic.Field(
        default=None, description="AWS credentials."
    )


class RequiredContainerConfig(pydantic.BaseModel):
    host: str = pydantic.Field(description="Host name.")
    aws_config: RequiredSubModel = pydantic.Field(description="AWS credentials.")


def _group_child(form, group_name, child_name):
    group = next(f for s in form.sections for f in s.fields if f.name == group_name)
    assert group.group_fields is not None
    return next(c for c in group.group_fields if c.name == child_name)


def test_optional_container_does_not_force_child_required() -> None:
    form = build_form(
        "optional_container", "optional_container", OptionalContainerConfig
    )
    child_field = _group_child(form, "aws_config", "aws_access_key_id")
    assert child_field.required is False


def test_required_container_keeps_child_required() -> None:
    form = build_form(
        "required_container", "required_container", RequiredContainerConfig
    )
    child_field = _group_child(form, "aws_config", "aws_access_key_id")
    assert child_field.required is True

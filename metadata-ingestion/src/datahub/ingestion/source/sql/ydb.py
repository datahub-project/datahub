# This import verifies that the dependencies are available.
import json
from typing import Any, Dict, Optional

import ydb_sqlalchemy  # noqa: F401
from pydantic import Field, SecretStr, model_validator
from sqlalchemy.engine.reflection import Inspector

from datahub.configuration.common import HiddenFromDocs
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.source.sql.sqlalchemy_uri import make_sqlalchemy_uri
from datahub.ingestion.source.sql.two_tier_sql_source import (
    TwoTierSQLAlchemyConfig,
    TwoTierSQLAlchemySource,
)


class YDBConfig(TwoTierSQLAlchemyConfig):
    scheme: HiddenFromDocs[str] = Field(
        default="yql+ydb", description="database scheme"
    )
    host_port: str = Field(default="localhost:2136", description="YDB endpoint URL.")
    database: str = Field(description="The YDB database to ingest, e.g. `/local`.")

    # YDB authenticates via the driver's `credentials` argument, not the URL, so
    # these are injected into `connect_args` below. Precedence: access_token >
    # service_account_json > service_account_key_file > username/password >
    # anonymous.
    access_token: Optional[SecretStr] = Field(
        default=None, description="Access token for IAM/OAuth authentication."
    )
    service_account_json: Optional[SecretStr] = Field(
        default=None, description="Service account key as JSON content."
    )
    service_account_key_file: Optional[str] = Field(
        default=None, description="Path to a service account key JSON file."
    )
    # Transport options, independent of the authentication method above.
    protocol: Optional[str] = Field(
        default=None, description="gRPC protocol, e.g. `grpc` or `grpcs`."
    )
    root_certificates_path: Optional[str] = Field(
        default=None,
        description="Path to the gRPC root CA certificate for TLS connections.",
    )
    root_certificates: Optional[str] = Field(
        default=None,
        description="gRPC root CA certificate content for TLS connections.",
    )

    def get_identifier(self, *, schema: str, table: str) -> str:
        # YDB has no schema layer — directory paths are part of the table name.
        return f"{schema}.{table}"

    @model_validator(mode="after")
    def _inject_ydb_credentials(self) -> "YDBConfig":
        # Pass credentials to the YDB driver programmatically via connect_args
        # (never through os.environ). The dict shapes are what ydb-dbapi expects.
        connect_args = self.options.setdefault("connect_args", {})
        if "credentials" not in connect_args:
            if self.access_token is not None:
                connect_args["credentials"] = {
                    "token": self.access_token.get_secret_value()
                }
            elif self.service_account_json is not None:
                # ydb-dbapi re-serializes this dict, so parse the JSON content here.
                connect_args["credentials"] = {
                    "service_account_json": json.loads(
                        self.service_account_json.get_secret_value()
                    )
                }
            elif self.service_account_key_file is not None:
                connect_args["credentials"] = {
                    "service_account_file": self.service_account_key_file
                }
            elif self.username is not None:
                connect_args["credentials"] = {
                    "username": self.username,
                    "password": self.password.get_secret_value()
                    if self.password is not None
                    else None,
                }
        # Transport options are orthogonal to credentials — applied regardless of
        # which (if any) authentication method is configured.
        if self.protocol and "protocol" not in connect_args:
            connect_args["protocol"] = self.protocol
        if self.root_certificates_path and "root_certificates_path" not in connect_args:
            connect_args["root_certificates_path"] = self.root_certificates_path
        if self.root_certificates and "root_certificates" not in connect_args:
            connect_args["root_certificates"] = self.root_certificates
        return self

    def get_sql_alchemy_url(
        self,
        uri_opts: Optional[Dict[str, Any]] = None,
        current_db: Optional[str] = None,
    ) -> str:
        if self.sqlalchemy_uri:
            return super().get_sql_alchemy_url(uri_opts=uri_opts, current_db=current_db)
        # Credentials are supplied to the driver via connect_args, so they are
        # deliberately omitted from the URL (YDB ignores URL-embedded user:pass,
        # and keeping them out avoids leaking secrets into debug logs).
        return make_sqlalchemy_uri(
            self.scheme,
            None,
            None,
            self.host_port,
            current_db or self.database,
            uri_opts=uri_opts,
        )


@platform_name("YDB")
@config_class(YDBConfig)
@support_status(SupportStatus.TESTING)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.DOMAINS, "Supported via the `domain` config field")
@capability(SourceCapability.DATA_PROFILING, "Optionally enabled via configuration")
class YDBSource(TwoTierSQLAlchemySource):
    """
    This plugin extracts the following:

    - Metadata for tables and views
    - Column types associated with each table/view
    """

    config: YDBConfig

    def __init__(self, config: YDBConfig, ctx: PipelineContext):
        super().__init__(config, ctx, self.get_platform())

    def get_platform(self):
        return "ydb"

    def get_db_name(self, inspector: Inspector) -> str:
        # YDB database paths are always rooted ("/local"). Normalize to the rooted
        # form so dataset URNs and containers faithfully reflect the real path and
        # are identical whether the user configures "local" or "/local".
        db_name = super().get_db_name(inspector)
        return db_name if db_name.startswith("/") else f"/{db_name}"

    @classmethod
    def create(cls, config_dict: Dict[str, Any], ctx: PipelineContext) -> "YDBSource":
        config = YDBConfig.model_validate(config_dict)
        return cls(config, ctx)

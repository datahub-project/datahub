# This import verifies that the dependencies are available.
import json
import sys
from importlib.machinery import EXTENSION_SUFFIXES
from importlib.util import find_spec
from typing import Any, Dict, Iterable, Optional

import ydb_sqlalchemy  # noqa: F401
from pydantic import Field, SecretStr, model_validator
from sqlalchemy import create_engine, inspect
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
from datahub.ingestion.source.common.subtypes import SourceCapabilityModifier
from datahub.ingestion.source.sql.sqlalchemy_uri import make_sqlalchemy_uri
from datahub.ingestion.source.sql.two_tier_sql_source import (
    TwoTierSQLAlchemyConfig,
    TwoTierSQLAlchemySource,
)
from datahub.sql_parsing.sqlglot_utils import get_dialect


class YDBConfig(TwoTierSQLAlchemyConfig):
    scheme: HiddenFromDocs[str] = Field(
        default="yql+ydb", description="database scheme"
    )
    host_port: str = Field(default="localhost:2136", description="YDB endpoint URL.")
    database: str = Field(description="The YDB database to ingest, e.g. `/local`.")
    include_view_lineage: bool = Field(
        default=False,
        description="Extract lineage from YDB view definitions. Requires "
        "ydb-sqlglot-plugin and pure-Python SQLGlot. Set "
        "DATAHUB_SQLGLOT_DISABLE_C=1 before starting the ingestion process.",
    )

    # YDB authenticates via the driver's `credentials` argument, not the URL, so
    # these are supplied via `connect_args` when connecting. Precedence: access_token >
    # service_account_key > service_account_key_file > username/password >
    # anonymous.
    access_token: Optional[SecretStr] = Field(
        default=None, description="Access token for IAM/OAuth authentication."
    )
    # The _key suffix also protects inline JSON in raw recipe reporting, which
    # runs before source validation and therefore cannot rely on SecretStr.
    service_account_key: Optional[SecretStr] = Field(
        default=None,
        description="Service account key as JSON content. For a file path, "
        "use service_account_key_file instead.",
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

    @classmethod
    def probe_provider_class(cls) -> None:
        # YDB needs a probe with explicit catalog scoping before it can expose
        # the SQL family's generic provider.
        return None

    def get_identifier(self, *, schema: str, table: str) -> str:
        # YDB has no schema layer — directory paths are part of the table name.
        return f"{schema}.{table}"

    @model_validator(mode="after")
    def _validate_view_lineage(self) -> "YDBConfig":
        if not self.include_view_lineage:
            return self
        if find_spec("ydb_sqlglot") is None:
            raise ValueError(
                "YDB view lineage requires ydb-sqlglot-plugin. "
                "Install the YDB ingestion extra with pip install 'acryl-datahub[ydb]'."
            )
        # Check loaded modules rather than the flag: setting it after SQLGlot
        # has been imported cannot replace its compiled classes safely.
        for name, module in sys.modules.copy().items():
            module_file = getattr(module, "__file__", None)
            if (
                name.startswith("sqlglot.")
                and isinstance(module_file, str)
                and module_file.endswith(tuple(EXTENSION_SUFFIXES))
            ):
                raise ValueError(
                    "YDB view lineage is incompatible with SQLGlot C extensions. "
                    "Set DATAHUB_SQLGLOT_DISABLE_C=1 before starting ingestion, "
                    "or set include_view_lineage=false to ingest metadata without lineage."
                )
        try:
            get_dialect("ydb").parse("SELECT 1")
        except Exception as exc:
            raise ValueError(
                "The YDB SQLGlot plugin could not parse SQL. Verify the installed "
                "ydb-sqlglot-plugin and SQLGlot versions, or set include_view_lineage=false."
            ) from exc
        return self

    def get_connection_options(self) -> Dict[str, Any]:
        # Keep plaintext credentials out of serializable config. Only the engine
        # receives this transient dictionary; self.options stays unchanged.
        options = dict(self.options)
        connect_args = dict(options.get("connect_args", {}))
        if "credentials" not in connect_args:
            if self.access_token is not None:
                connect_args["credentials"] = {
                    "token": self.access_token.get_secret_value()
                }
            elif self.service_account_key is not None:
                # ydb-dbapi re-serializes this dict, so parse the JSON content here.
                key_content = json.loads(self.service_account_key.get_secret_value())
                if not isinstance(key_content, dict):
                    raise ValueError("service_account_key must contain a JSON object")
                connect_args["credentials"] = {"service_account_json": key_content}
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
        options["connect_args"] = connect_args
        return options

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
@support_status(SupportStatus.ALPHA)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.DOMAINS, "Supported via the `domain` config field")
@capability(SourceCapability.DATA_PROFILING, "Optionally enabled via configuration")
@capability(
    SourceCapability.LINEAGE_COARSE,
    "Opt-in via include_view_lineage; requires the YDB SQLGlot plugin and "
    "DATAHUB_SQLGLOT_DISABLE_C=1 before process startup",
    subtype_modifier=[SourceCapabilityModifier.VIEW],
)
@capability(
    SourceCapability.LINEAGE_FINE,
    "Requires include_view_lineage and include_view_column_lineage; uses "
    "the same pure-Python SQLGlot runtime as view lineage",
    subtype_modifier=[SourceCapabilityModifier.VIEW],
)
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

    def get_inspectors(self) -> Iterable[Inspector]:
        if not self.config.database_pattern.allowed(self.config.database):
            return
        # A YDB connection is bound to one required database. The profiler uses
        # this same connection, so credentials never need to enter config.options.
        engine = create_engine(
            self.config.get_sql_alchemy_url(current_db=self.config.database),
            **self.config.get_connection_options(),
        )
        try:
            with engine.connect() as connection:
                yield inspect(connection)
        finally:
            engine.dispose()

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

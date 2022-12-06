import logging
from typing import Dict, Optional

import pydantic
import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from snowflake.connector.network import (
    DEFAULT_AUTHENTICATOR,
    EXTERNAL_BROWSER_AUTHENTICATOR,
    KEY_PAIR_AUTHENTICATOR,
    OAUTH_AUTHENTICATOR,
)

from datahub.configuration.common import (
    AllowDenyPattern,
    ConfigModel,
    ConfigurationError,
    OauthConfiguration,
)
from datahub.configuration.time_window_config import BaseTimeWindowConfig
from datahub.ingestion.source.sql.oauth_generator import OauthTokenGenerator
from datahub.ingestion.source.sql.sql_common import (
    SQLAlchemyConfig,
    make_sqlalchemy_uri,
)
from datahub.utilities.config_clean import (
    remove_protocol,
    remove_suffix,
    remove_trailing_slashes,
)

logger: logging.Logger = logging.getLogger(__name__)

APPLICATION_NAME: str = "acryl_datahub"

VALID_AUTH_TYPES: Dict[str, str] = {
    "DEFAULT_AUTHENTICATOR": DEFAULT_AUTHENTICATOR,
    "EXTERNAL_BROWSER_AUTHENTICATOR": EXTERNAL_BROWSER_AUTHENTICATOR,
    "KEY_PAIR_AUTHENTICATOR": KEY_PAIR_AUTHENTICATOR,
    "OAUTH_AUTHENTICATOR": OAUTH_AUTHENTICATOR,
}


class SnowflakeProvisionRoleConfig(ConfigModel):
    enabled: bool = pydantic.Field(
        default=False,
        description="Whether provisioning of Snowflake role (used for ingestion) is enabled or not.",
    )

    # Can be used by account admin to test what sql statements will be run
    dry_run: bool = pydantic.Field(
        default=False,
        description="If provision_role is enabled, whether to dry run the sql commands for system admins to see what sql grant commands would be run without actually running the grant commands.",
    )

    # Setting this to True is helpful in case you want a clean role without any extra privileges
    # Not set to True by default because multiple parallel
    #   snowflake ingestions can be dependent on single role
    drop_role_if_exists: bool = pydantic.Field(
        default=False,
        description="Useful during testing to ensure you have a clean slate role. Not recommended for production use cases.",
    )

    # When Account admin is testing they might not want to actually do the ingestion
    # Set this to False in case the account admin would want to
    #   create role
    #   grant role to user in main config
    #   run ingestion as the user in main config
    run_ingestion: bool = pydantic.Field(
        default=False,
        description="If system admins wish to skip actual ingestion of metadata during testing of the provisioning of role.",
    )

    admin_role: Optional[str] = pydantic.Field(
        default="accountadmin",
        description="The Snowflake role of admin user used for provisioning of the role specified by role config. System admins can audit the open source code and decide to use a different role.",
    )

    admin_username: str = pydantic.Field(
        description="The username to be used for provisioning of role."
    )

    admin_password: pydantic.SecretStr = pydantic.Field(
        default=None,
        exclude=True,
        description="The password to be used for provisioning of role.",
    )

    @pydantic.validator("admin_username", always=True)
    def username_not_empty(cls, v, values, **kwargs):
        v_str: str = str(v)
        if not v_str.strip():
            raise ValueError("username is empty")
        return v


class BaseSnowflakeConfig(BaseTimeWindowConfig):
    # Note: this config model is also used by the snowflake-usage source.

    scheme: str = "snowflake"
    username: Optional[str] = pydantic.Field(
        default=None, description="Snowflake username."
    )
    password: Optional[pydantic.SecretStr] = pydantic.Field(
        default=None, exclude=True, description="Snowflake password."
    )
    private_key: Optional[str] = pydantic.Field(
        default=None,
        description="Private key in a form of '-----BEGIN PRIVATE KEY-----\\nprivate-key\\n-----END PRIVATE KEY-----\\n' if using key pair authentication. Encrypted version of private key will be in a form of '-----BEGIN ENCRYPTED PRIVATE KEY-----\\nencrypted-private-key\\n-----END ECNCRYPTED PRIVATE KEY-----\\n' See: https://docs.snowflake.com/en/user-guide/key-pair-auth.html",
    )

    private_key_path: Optional[str] = pydantic.Field(
        default=None,
        description="The path to the private key if using key pair authentication. Ignored if `private_key` is set. See: https://docs.snowflake.com/en/user-guide/key-pair-auth.html",
    )
    private_key_password: Optional[pydantic.SecretStr] = pydantic.Field(
        default=None,
        exclude=True,
        description="Password for your private key. Required if using key pair authentication with encrypted private key.",
    )

    oauth_config: Optional[OauthConfiguration] = pydantic.Field(
        default=None,
        description="oauth configuration - https://docs.snowflake.com/en/user-guide/python-connector-example.html#connecting-with-oauth",
    )
    authentication_type: str = pydantic.Field(
        default="DEFAULT_AUTHENTICATOR",
        description='The type of authenticator to use when connecting to Snowflake. Supports "DEFAULT_AUTHENTICATOR", "EXTERNAL_BROWSER_AUTHENTICATOR" and "KEY_PAIR_AUTHENTICATOR".',
    )
    host_port: Optional[str] = pydantic.Field(
        description="DEPRECATED: Snowflake account. e.g. abc48144"
    )  # Deprecated
    account_id: Optional[str] = pydantic.Field(
        description="Snowflake account identifier. e.g. xy12345,  xy12345.us-east-2.aws, xy12345.us-central1.gcp, xy12345.central-us.azure. Refer [Account Identifiers](https://docs.snowflake.com/en/user-guide/admin-account-identifier.html#format-2-legacy-account-locator-in-a-region) for more details."
    )  # Once host_port is removed this will be made mandatory
    warehouse: Optional[str] = pydantic.Field(description="Snowflake warehouse.")
    role: Optional[str] = pydantic.Field(description="Snowflake role.")
    include_table_lineage: bool = pydantic.Field(
        default=True,
        description="If enabled, populates the snowflake table-to-table and s3-to-snowflake table lineage. Requires appropriate grants given to the role.",
    )
    include_view_lineage: bool = pydantic.Field(
        default=True,
        description="If enabled, populates the snowflake view->table and table->view lineages (no view->view lineage yet). Requires appropriate grants given to the role, and include_table_lineage to be True.",
    )
    connect_args: Optional[Dict] = pydantic.Field(
        default=None,
        description="Connect args to pass to Snowflake SqlAlchemy driver",
        exclude=True,
    )
    check_role_grants: bool = pydantic.Field(
        default=False,
        description="If set to True then checks role grants at the beginning of the ingestion run. To be used for debugging purposes. If you think everything is working fine then set it to False. In some cases this can take long depending on how many roles you might have.",
    )

    def get_account(self) -> str:
        assert self.account_id
        return self.account_id

    @pydantic.root_validator
    def one_of_host_port_or_account_id_is_required(cls, values):
        host_port = values.get("host_port")
        if host_port is not None:
            logger.warning(
                "snowflake's `host_port` option has been deprecated; use account_id instead"
            )
            host_port = remove_protocol(host_port)
            host_port = remove_trailing_slashes(host_port)
            host_port = remove_suffix(host_port, ".snowflakecomputing.com")
            values["host_port"] = host_port
        account_id = values.get("account_id")
        if account_id is None:
            if host_port is None:
                raise ConfigurationError(
                    "One of account_id (recommended) or host_port (deprecated) is required"
                )
            else:
                values["account_id"] = host_port
        return values

    @pydantic.validator("authentication_type", always=True)
    def authenticator_type_is_valid(cls, v, values, field):
        if v not in VALID_AUTH_TYPES.keys():
            raise ValueError(
                f"unsupported authenticator type '{v}' was provided,"
                f" use one of {list(VALID_AUTH_TYPES.keys())}"
            )
        if v == "KEY_PAIR_AUTHENTICATOR":
            # If we are using key pair auth, we need the private key path and password to be set
            if (
                values.get("private_key") is None
                and values.get("private_key_path") is None
            ):
                raise ValueError(
                    f"Both `private_key` and `private_key_path` are none. "
                    f"At least one should be set when using {v} authentication"
                )
        elif v == "OAUTH_AUTHENTICATOR":
            if values.get("oauth_config") is None:
                raise ValueError(
                    f"'oauth_config' is none but should be set when using {v} authentication"
                )
            if values.get("oauth_config").provider is None:
                raise ValueError(
                    f"'oauth_config.provider' is none "
                    f"but should be set when using {v} authentication"
                )
            if values.get("oauth_config").client_id is None:
                raise ValueError(
                    f"'oauth_config.client_id' is none "
                    f"but should be set when using {v} authentication"
                )
            if values.get("oauth_config").scopes is None:
                raise ValueError(
                    f"'oauth_config.scopes' was none "
                    f"but should be set when using {v} authentication"
                )
            if values.get("oauth_config").authority_url is None:
                raise ValueError(
                    f"'oauth_config.authority_url' was none "
                    f"but should be set when using {v} authentication"
                )
            if values.get("oauth_config").use_certificate is True:
                if values.get("oauth_config").encoded_oauth_private_key is None:
                    raise ValueError(
                        "'base64_encoded_oauth_private_key' was none "
                        "but should be set when using certificate for oauth_config"
                    )
                if values.get("oauth").encoded_oauth_public_key is None:
                    raise ValueError(
                        "'base64_encoded_oauth_public_key' was none"
                        "but should be set when using use_certificate true for oauth_config"
                    )
            elif values.get("oauth_config").client_secret is None:
                raise ValueError(
                    "'oauth_config.client_secret' was none "
                    "but should be set when using use_certificate false for oauth_config"
                )
        logger.info(f"using authenticator type '{v}'")
        return v

    @pydantic.validator("include_view_lineage")
    def validate_include_view_lineage(cls, v, values):
        if not values.get("include_table_lineage") and v:
            raise ValueError(
                "include_table_lineage must be True for include_view_lineage to be set."
            )
        return v

    def get_sql_alchemy_url(
        self,
        database: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[pydantic.SecretStr] = None,
        role: Optional[str] = None,
    ) -> str:
        if username is None:
            username = self.username
        if password is None:
            password = self.password
        if role is None:
            role = self.role
        return make_sqlalchemy_uri(
            self.scheme,
            username,
            password.get_secret_value() if password else None,
            self.account_id,
            f'"{database}"' if database is not None else database,
            uri_opts={
                # Drop the options if value is None.
                key: value
                for (key, value) in {
                    "authenticator": VALID_AUTH_TYPES.get(self.authentication_type),
                    "warehouse": self.warehouse,
                    "role": role,
                    "application": APPLICATION_NAME,
                }.items()
                if value
            },
        )

    def get_sql_alchemy_connect_args(self) -> dict:
        if self.authentication_type != "KEY_PAIR_AUTHENTICATOR":
            return {}
        if self.connect_args is None:
            if self.private_key is not None:
                pkey_bytes = self.private_key.replace("\\n", "\n").encode()
            else:
                assert (
                    self.private_key_path
                ), "missing required private key path to read key from"
                with open(self.private_key_path, "rb") as key:
                    pkey_bytes = key.read()

            p_key = serialization.load_pem_private_key(
                pkey_bytes,
                password=self.private_key_password.get_secret_value().encode()
                if self.private_key_password is not None
                else None,
                backend=default_backend(),
            )

            pkb = p_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            )
            self.connect_args = {"private_key": pkb}
        return self.connect_args


class SnowflakeConfig(BaseSnowflakeConfig, SQLAlchemyConfig):
    database_pattern: AllowDenyPattern = AllowDenyPattern(
        deny=[r"^UTIL_DB$", r"^SNOWFLAKE$", r"^SNOWFLAKE_SAMPLE_DATA$"]
    )

    provision_role: Optional[SnowflakeProvisionRoleConfig] = None
    ignore_start_time_lineage: bool = False
    upstream_lineage_in_report: bool = False

    def get_sql_alchemy_url(
        self,
        database: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[pydantic.SecretStr] = None,
        role: Optional[str] = None,
    ) -> str:
        return super().get_sql_alchemy_url(
            database=database, username=username, password=password, role=role
        )

    def get_options(self) -> dict:
        options_connect_args: Dict = super().get_sql_alchemy_connect_args()
        options_connect_args.update(self.options.get("connect_args", {}))
        self.options["connect_args"] = options_connect_args
        if self.connect_args is not None:
            self.options["connect_args"].update(self.connect_args)
        return self.options

    def get_oauth_connection(self):
        assert (
            self.oauth_config
        ), "oauth_config should be provided if using oauth based authentication"
        generator = OauthTokenGenerator(
            self.oauth_config.client_id,
            self.oauth_config.authority_url,
            self.oauth_config.provider,
        )
        if self.oauth_config.use_certificate:
            response = generator.get_token_with_certificate(
                private_key_content=str(self.oauth_config.encoded_oauth_public_key),
                public_key_content=str(self.oauth_config.encoded_oauth_private_key),
                scopes=self.oauth_config.scopes,
            )
        else:
            response = generator.get_token_with_secret(
                secret=str(self.oauth_config.client_secret),
                scopes=self.oauth_config.scopes,
            )
        token = response["access_token"]
        connect_args = self.get_options()["connect_args"]
        return snowflake.connector.connect(
            user=self.username,
            account=self.account_id,
            token=token,
            warehouse=self.warehouse,
            authenticator=VALID_AUTH_TYPES.get(self.authentication_type),
            application=APPLICATION_NAME,
            **connect_args,
        )

    def get_key_pair_connection(self) -> snowflake.connector.SnowflakeConnection:
        connect_args = self.get_options()["connect_args"]

        return snowflake.connector.connect(
            user=self.username,
            account=self.account_id,
            warehouse=self.warehouse,
            role=self.role,
            authenticator=VALID_AUTH_TYPES.get(self.authentication_type),
            application=APPLICATION_NAME,
            **connect_args,
        )

    def get_connection(self) -> snowflake.connector.SnowflakeConnection:
        connect_args = self.get_options()["connect_args"]
        if self.authentication_type == "DEFAULT_AUTHENTICATOR":
            return snowflake.connector.connect(
                user=self.username,
                password=self.password.get_secret_value() if self.password else None,
                account=self.account_id,
                warehouse=self.warehouse,
                role=self.role,
                application=APPLICATION_NAME,
                **connect_args,
            )
        elif self.authentication_type == "OAUTH_AUTHENTICATOR":
            return self.get_oauth_connection()
        elif self.authentication_type == "KEY_PAIR_AUTHENTICATOR":
            return self.get_key_pair_connection()
        elif self.authentication_type == "EXTERNAL_BROWSER_AUTHENTICATOR":
            return snowflake.connector.connect(
                user=self.username,
                password=self.password.get_secret_value() if self.password else None,
                account=self.account_id,
                warehouse=self.warehouse,
                role=self.role,
                authenticator=VALID_AUTH_TYPES.get(self.authentication_type),
                application=APPLICATION_NAME,
                **connect_args,
            )
        else:
            # not expected to be here
            raise Exception("Not expected to be here.")

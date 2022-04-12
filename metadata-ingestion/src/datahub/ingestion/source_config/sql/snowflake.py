import logging
from typing import Optional

import pydantic
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from snowflake.connector.network import (
    DEFAULT_AUTHENTICATOR,
    EXTERNAL_BROWSER_AUTHENTICATOR,
    KEY_PAIR_AUTHENTICATOR,
    OAUTH_AUTHENTICATOR,
)

from datahub.configuration.common import AllowDenyPattern, ConfigModel
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

APPLICATION_NAME = "acryl_datahub"

logger: logging.Logger = logging.getLogger(__name__)


class SnowflakeProvisionRoleConfig(ConfigModel):
    enabled: bool = False

    # Can be used by account admin to test what sql statements will be run
    dry_run: bool = False

    # Setting this to True is helpful in case you want a clean role without any extra privileges
    # Not set to True by default because multiple parallel
    #   snowflake ingestions can be dependent on single role
    drop_role_if_exists: bool = False

    # When Account admin is testing they might not want to actually do the ingestion
    # Set this to False in case the account admin would want to
    #   create role
    #   grant role to user in main config
    #   run ingestion as the user in main config
    run_ingestion: bool = False

    admin_role: Optional[str] = "accountadmin"

    admin_username: str
    admin_password: pydantic.SecretStr = pydantic.Field(default=None, exclude=True)

    @pydantic.validator("admin_username", always=True)
    def username_not_empty(cls, v, values, **kwargs):
        v_str: str = str(v)
        if v_str.strip() == "":
            raise ValueError("username is empty")
        return v


class BaseSnowflakeConfig(BaseTimeWindowConfig):
    # Note: this config model is also used by the snowflake-usage source.

    scheme = "snowflake"

    username: Optional[str] = None
    use_certificate: Optional[bool] = False
    oauth_client_id: Optional[str]
    oauth_scope: Optional[str]
    oauth_client_secret: Optional[str]
    oauth_authority_url: Optional[str]
    base64_encoded_oauth_public_key: Optional[str]
    base64_encoded_oauth_private_key: Optional[str]
    password: Optional[pydantic.SecretStr] = pydantic.Field(default=None, exclude=True)
    private_key_path: Optional[str]
    private_key_password: Optional[pydantic.SecretStr] = pydantic.Field(
        default=None, exclude=True
    )
    authentication_type: Optional[str] = "DEFAULT_AUTHENTICATOR"
    host_port: str
    warehouse: Optional[str]
    role: Optional[str]
    include_table_lineage: Optional[bool] = True
    include_view_lineage: Optional[bool] = True

    connect_args: Optional[dict]

    @pydantic.validator("host_port", always=True)
    def host_port_is_valid(cls, v, values, **kwargs):
        v = remove_protocol(v)
        v = remove_trailing_slashes(v)
        v = remove_suffix(v, ".snowflakecomputing.com")
        return v

    @pydantic.validator("authentication_type", always=True)
    def authenticator_type_is_valid(cls, v, values, **kwargs):
        valid_auth_types = {
            "DEFAULT_AUTHENTICATOR": DEFAULT_AUTHENTICATOR,
            "EXTERNAL_BROWSER_AUTHENTICATOR": EXTERNAL_BROWSER_AUTHENTICATOR,
            "KEY_PAIR_AUTHENTICATOR": KEY_PAIR_AUTHENTICATOR,
            "OAUTH_AUTHENTICATOR": OAUTH_AUTHENTICATOR,
        }
        if v not in valid_auth_types.keys():
            raise ValueError(
                f"unsupported authenticator type '{v}' was provided,"
                f" use one of {list(valid_auth_types.keys())}"
            )
        else:
            if v == "KEY_PAIR_AUTHENTICATOR":
                # If we are using key pair auth, we need the private key path and password to be set
                if values.get("private_key_path") is None:
                    raise ValueError(
                        f"'private_key_path' was none "
                        f"but should be set when using {v} authentication"
                    )
                if values.get("private_key_password") is None:
                    raise ValueError(
                        f"'private_key_password' was none "
                        f"but should be set when using {v} authentication"
                    )
            elif v == "OAUTH_AUTHENTICATOR":
                if values.get("oauth_client_id") is None:
                    raise ValueError(
                        f"'oauth_client_id' is none "
                        f"but should be set when using {v} authentication"
                    )
                if values.get("oauth_scope") is None:
                    raise ValueError(
                        f"'oauth_scope' was none "
                        f"but should be set when using {v} authentication"
                    )
                if values.get(oauth_authority_url) is None:
                    raise ValueError(
                        f"'oauth_authority_url' was none "
                        f"but should be set when using {v} authentication"
                    )
                if values.get("use_certificate") == True:
                    if values.get("base64_encoded_oauth_private_key") is None:
                        raise ValueError(
                            f"'base64_encoded_oauth_private_key' was none "
                            f"but should be set when using {v} authentication"
                        )
                    if values.get("base64_encoded_oauth_public_key") is None:
                        raise ValueError(
                            f"'base64_encoded_oauth_public_key' was none"
                            f"but should be set when using {v} authentication"
                        )
                else:
                    if values.get("oauth_client_secret") is None:
                        raise ValueError(
                            f"'oauth_client_secret' was none "
                            f"but should be set when using {v} authentication"
                        )
            logger.info(f"using authenticator type '{v}'")
        return valid_auth_types.get(v)

    @pydantic.validator("include_view_lineage")
    def validate_include_view_lineage(cls, v, values):
        if not values.get("include_table_lineage") and v:
            raise ValueError(
                "include_table_lineage must be True for include_view_lineage to be set."
            )
        return v

    def get_oauth_connection(self):
        generator = OauthTokenGenerator(self.oauth_client_id,self.oauth_authority_url)
        if self.use_certificate is True:
            response = generator.get_token_with_certificate(
                private_key_content=self.base64_encoded_oauth_private_key,
                public_key_content=self.base64_encoded_oauth_public_key,
                scope=self.oauth_scope,
            )
        else:
            response = generator.get_token_with_secret(secret=self.oauth_client_secret,scope=self.oauth_scope)
        token = response["access_token"]

        return snowflake.connector.connect(
            user=self.username,
            account=self.host_port,
            authenticator="oauth",                          
            token=token,
            warehouse=self.warehouse                    
        )    
    
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
            self.host_port,
            f'"{database}"' if database is not None else database,
            uri_opts={
                # Drop the options if value is None.
                key: value
                for (key, value) in {
                    "authenticator": self.authentication_type,
                    "warehouse": self.warehouse,
                    "role": role,
                    "application": APPLICATION_NAME,
                }.items()
                if value
            },
        )

    def get_sql_alchemy_connect_args(self) -> dict:
        if self.authentication_type != KEY_PAIR_AUTHENTICATOR:
            return {}
        if self.connect_args is None:
            if self.private_key_path is None:
                raise ValueError("missing required private key path to read key from")
            if self.private_key_password is None:
                raise ValueError("missing required private key password")
            with open(self.private_key_path, "rb") as key:
                p_key = serialization.load_pem_private_key(
                    key.read(),
                    password=self.private_key_password.get_secret_value().encode(),
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

    database: Optional[str]  # deprecated

    provision_role: Optional[SnowflakeProvisionRoleConfig] = None
    ignore_start_time_lineage: bool = False
    report_upstream_lineage: bool = False

    @pydantic.validator("database")
    def note_database_opt_deprecation(cls, v, values, **kwargs):
        logger.warning(
            "snowflake's `database` option has been deprecated; use database_pattern instead"
        )
        values["database_pattern"].allow = f"^{v}$"
        return None

    def get_sql_alchemy_url(
        self,
        database: str = None,
        username: Optional[str] = None,
        password: Optional[pydantic.SecretStr] = None,
        role: Optional[str] = None,
    ) -> str:
        return super().get_sql_alchemy_url(
            database=database, username=username, password=password, role=role
        )

    def get_sql_alchemy_connect_args(self) -> dict:
        return super().get_sql_alchemy_connect_args()

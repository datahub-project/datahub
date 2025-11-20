from typing import Dict, Literal

from snowflake.connector.network import (
    DEFAULT_AUTHENTICATOR,
    EXTERNAL_BROWSER_AUTHENTICATOR,
    KEY_PAIR_AUTHENTICATOR,
    OAUTH_AUTHENTICATOR,
)

# Type-safe alias for authentication types
AuthenticationType = Literal[
    "DEFAULT_AUTHENTICATOR",
    "EXTERNAL_BROWSER_AUTHENTICATOR",
    "KEY_PAIR_AUTHENTICATOR",
    "OAUTH_AUTHENTICATOR",
]

# Authentication type configuration keys (stored in JSON configs)
AUTH_TYPE_DEFAULT: AuthenticationType = "DEFAULT_AUTHENTICATOR"
AUTH_TYPE_EXTERNAL_BROWSER: AuthenticationType = "EXTERNAL_BROWSER_AUTHENTICATOR"
AUTH_TYPE_KEY_PAIR: AuthenticationType = "KEY_PAIR_AUTHENTICATOR"
AUTH_TYPE_OAUTH: AuthenticationType = "OAUTH_AUTHENTICATOR"

# Mapping from config keys to Snowflake library constants
VALID_AUTH_TYPES: Dict[AuthenticationType, str] = {
    AUTH_TYPE_DEFAULT: DEFAULT_AUTHENTICATOR,
    AUTH_TYPE_EXTERNAL_BROWSER: EXTERNAL_BROWSER_AUTHENTICATOR,
    AUTH_TYPE_KEY_PAIR: KEY_PAIR_AUTHENTICATOR,
    AUTH_TYPE_OAUTH: OAUTH_AUTHENTICATOR,
}

# Default authentication type
DEFAULT_AUTH_TYPE: AuthenticationType = "DEFAULT_AUTHENTICATOR"

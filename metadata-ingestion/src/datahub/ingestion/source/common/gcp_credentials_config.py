import json
import tempfile
from typing import Any, Dict, Optional

from pydantic import Field, root_validator

from datahub.configuration import ConfigModel
from datahub.configuration.validate_multiline_string import pydantic_multiline_string


class GCPCredential(ConfigModel):
    project_id: Optional[str] = Field(description="Project id to set the credentials")
    private_key_id: str = Field(description="Private key id")
    private_key: str = Field(
        description="Private key in a form of '-----BEGIN PRIVATE KEY-----\\nprivate-key\\n-----END PRIVATE KEY-----\\n'"
    )
    client_email: str = Field(description="Client email")
    client_id: str = Field(description="Client Id")
    auth_uri: str = Field(
        default="https://accounts.google.com/o/oauth2/auth",
        description="Authentication uri",
    )
    token_uri: str = Field(
        default="https://oauth2.googleapis.com/token", description="Token uri"
    )
    auth_provider_x509_cert_url: str = Field(
        default="https://www.googleapis.com/oauth2/v1/certs",
        description="Auth provider x509 certificate url",
    )
    type: str = Field(default="service_account", description="Authentication type")
    client_x509_cert_url: Optional[str] = Field(
        default=None,
        description="If not set it will be default to https://www.googleapis.com/robot/v1/metadata/x509/client_email",
    )

    _fix_private_key_newlines = pydantic_multiline_string("private_key")

    @root_validator(skip_on_failure=True)
    def validate_config(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        if values.get("client_x509_cert_url") is None:
            values["client_x509_cert_url"] = (
                f"https://www.googleapis.com/robot/v1/metadata/x509/{values['client_email']}"
            )
        return values

    def create_credential_temp_file(self, project_id: Optional[str] = None) -> str:
        configs = self.dict()
        if project_id:
            configs["project_id"] = project_id
        with tempfile.NamedTemporaryFile(delete=False) as fp:
            cred_json = json.dumps(configs, indent=4, separators=(",", ": "))
            fp.write(cred_json.encode())
            return fp.name

    def to_dict(self, project_id: Optional[str] = None) -> Dict[str, str]:
        configs = self.dict()
        if project_id:
            configs["project_id"] = project_id
        return configs

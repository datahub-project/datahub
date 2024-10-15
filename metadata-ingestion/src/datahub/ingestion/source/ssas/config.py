from typing import List, Dict

import datahub.emitter.mce_builder as builder
import pydantic
from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.source_common import EnvConfigMixin


class SsasServerHTTPConfig(EnvConfigMixin):
    """
    Class represent config object.
    Contains parameters for connect to ssas over http (HTTP Access to Analysis Services)
    https://learn.microsoft.com/en-us/analysis-services/instances/configure-http-access-to-analysis-services-on-iis-8-0?view=asallproducts-allversions

    username - Active Directory user login
    username - Active Directory user password
    host_port - XMLA gateway url (format - url:port)
    server_alias - XMLA gateway server alias
    virtual_directory_name -
    instance -  not used ???
    use_https - set true if use XMLA gateway over https
    dns_suffixes - list dns zone if use ssas servers in different domains.
        Used to search for the main domain for the ssas server if it is not specified in the cube properties

    """
    username: str = pydantic.Field(description="Windows account username")
    password: str = pydantic.Field(description="Windows account password")
    instance: str
    host_port: str = pydantic.Field(
        default="localhost:81", description="XMLA gateway url"
    )
    server_alias: str = pydantic.Field(default="localhost")

    virtual_directory_name: str = pydantic.Field(
        default="ssas", description="Report Virtual Directory URL name"
    )
    ssas_instance: str
    use_https: bool = pydantic.Field(default=True)
    ssas_instance_auth_type: str = pydantic.Field(default="HTTPKerberosAuth", description="SSAS instance auth type")

    dns_suffixes: List = pydantic.Field(default_factory=list)
    default_ssas_instances_by_server: Dict = pydantic.Field(default_factory=dict)

    @pydantic.validator('ssas_instance_auth_type')
    def check_ssas_instance_auth_type(cls, v):
        if v not in ["HTTPBasicAuth", "HTTPKerberosAuth"]:
            raise ValueError("Support only HTTPBasicAuth or HTTPKerberosAuth auth type")
        return v

    @property
    def use_dns_resolver(self) -> bool:
        return bool(self.dns_suffixes)

    @property
    def base_api_url(self) -> str:
        protocol = "https" if self.use_https else "http"
        return f"{protocol}://{self.host_port}/{self.virtual_directory_name}/{self.ssas_instance}/msmdpump.dll"

    @property
    def host(self) -> str:
        return self.server_alias or self.host_port.split(":")[0]


class SsasServerHTTPSourceConfig(SsasServerHTTPConfig):
    platform_name: str = "ssas"
    platform_urn: str = builder.make_data_platform_urn(platform=platform_name)
    report_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()
    chart_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()

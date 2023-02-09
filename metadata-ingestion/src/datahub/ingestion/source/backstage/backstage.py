import logging
import os
import yaml
import json
import uuid
from abc import ABC
from typing import Dict, Generator, Iterable, Optional

from pydantic.fields import Field

from datahub.configuration.common import ConfigModel
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.backstage.backstage_parser import (
    get_swag_json,
    get_tok,
    request_call,
    get_swagger_list,
)
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent

logger: logging.Logger = logging.getLogger(__name__)

class BackstageConfig(ConfigModel):
    name: str = Field(description="")
    url: str = Field(description="")
    username: str = Field(default="", description="")
    password: str = Field(default="", description="")
    token: Optional[str] = Field(default=None, description="")
    get_token: dict = Field(default={}, description="")
    default_api_url: str = Field(default="", description="")
    default_ignore_endpoints: list = Field(default=[], description="")

    def get_swagger(self) -> Dict:
        if self.get_token or self.token is not None:
            if self.token is not None:
                ...
            else:
                assert (
                        "url_complement" in self.get_token.keys()
                ), "When 'request_type' is set to 'get', an url_complement is needed for the request."
                if self.get_token["request_type"] == "get":
                    assert (
                            "{username}" in self.get_token["url_complement"]
                    ), "we expect the keyword {username} to be present in the url"
                    assert (
                            "{password}" in self.get_token["url_complement"]
                    ), "we expect the keyword {password} to be present in the url"
                    url4req = self.get_token["url_complement"].replace(
                        "{username}", self.username
                    )
                    url4req = url4req.replace("{password}", self.password)
                elif self.get_token["request_type"] == "post":
                    url4req = self.get_token["url_complement"]
                else:
                    raise KeyError(
                        "This tool accepts only 'get' and 'post' as method for getting tokens"
                    )
                self.token = get_tok(
                    url=self.url,
                    username=self.username,
                    password=self.password,
                    tok_url=url4req,
                    method=self.get_token["request_type"],
                )
            sw_dict = get_swag_json(
                self.url, token=self.token,  # swagger_file=self.swagger_file
            )  # load the swagger file

        else:  # using basic auth for accessing endpoints
            sw_dict = get_swag_json(
                self.url,
                username=self.username,
                password=self.password,
                # swagger_file=self.swagger_file,
            )
        return sw_dict

class BackstageWorkUnit(MetadataWorkUnit):
    pass


@platform_name("Backstage", id="backstage")
@config_class(BackstageConfig)
@support_status(SupportStatus.CERTIFIED)
@capability(SourceCapability.PLATFORM_INSTANCE, supported=False, description="")
class BackstageSource(Source, ABC):
    def __init__(self, config: BackstageConfig, ctx: PipelineContext, platform: str):
        super().__init__(ctx)
        self.config = config
        self.platform = platform
        self.report = SourceReport()
        self.url_basepath = ""

    def report_bad_responses(self, status_code: int, key: str) -> None:
        if status_code == 400:
            self.report.report_warning(
                key=key, reason="Unknown error trying to reach the Backstage.io catalog!"
            )
        elif status_code == 403 or status_code == 401:
            self.report.report_warning(key=key, reason="Not authorised to access the Backstage.io catalog!")
        elif status_code == 404:
            self.report.report_warning(
                key=key,
                reason="Backstage.io catalog not available on the provided URL!",
            )
        elif status_code == 500:
            self.report.report_warning(
                key=key, reason="Server error for reaching the Backstage.io catalog!"
            )
        elif status_code == 504:
            self.report.report_warning(key=key, reason="Timeout for reaching the Backstage.io catalog!")
        else:
            raise Exception(
                f"Unable to retrieve the Backstage.io catalog, response code {status_code}, key {key}"
            )

    def build_wu(
            self, dataset_snapshot: DatasetSnapshot, dataset_name: str
    ) -> Generator[BackstageWorkUnit, None, None]:
        mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
        wu = BackstageWorkUnit(id=dataset_name, mce=mce)
        self.report.report_workunit(wu)
        yield wu

    def get_workunits(self) -> Iterable[BackstageWorkUnit]:  # noqa: C901

        # python object from json
        sw_dict = self.config.get_swagger()
        # try to apply logic and parse swagger strings
        swagger_content_list = get_swagger_list(sw_dict)

        # take care about empty url parameter that can cause problem with open api processing
        ctx = self.__getattribute__("ctx")
        default_api_url = self.config.default_api_url
        if not default_api_url:
            default_api_url = ctx.pipeline_config.source.config['url']
            default_api_url = "/".join(default_api_url.split("/")[:-1]) + '/'
        default_ignore_endpoints = self.config.default_ignore_endpoints
        backend_url = ctx.pipeline_config.datahub_api.server
        count = 0
        for line in swagger_content_list:
            try:
                sw_dict = json.loads(line)
            except json.JSONDecodeError:  # it's not a JSON!
                sw_dict = yaml.safe_load(line)

            folder_unique_name = uuid.uuid1().hex
            folder_name = '/tmp/datahub/ingest/' + folder_unique_name

            if "info" in sw_dict:
                info = sw_dict["info"]
                if "title" in info:
                    api_name = sw_dict["info"]["title"]
                else:
                    ctx = self.__getattribute__("ctx")
                    source_config_name = ctx.pipeline_config.source.config['name']
                    api_name = ""
                    print(
                        "API name for " + source_config_name + " is not available in info.title section of the swagger, this is a required parameter!")
                    logger.error(
                        "API name for " + source_config_name + " is not available in info.title section of the swagger, this is a required parameter!"
                    )
            else:
                ctx = self.__getattribute__("ctx")
                source_config_name = ctx.pipeline_config.source.config['name']
                api_name = ""
                print(
                    "Cannot retrieve the API name for " + source_config_name + " - section 'info' is missing; API name is a required parameter!")
                logger.error(
                    "Cannot retrieve the API name for " + source_config_name + " - section 'info' is missing; API name is a required parameter!"
                )

            os.makedirs(folder_name, exist_ok=True)
            raw_config = {
                "source":
                    {"type": "openapi",
                     "config":
                         {"name": api_name,
                          "url": default_api_url,
                          "swagger_file": line,
                          "ignore_endpoints": default_ignore_endpoints,
                          "swagger_embedded": True,
                          }
                     },
                "sink":
                    {"type": "datahub-rest",
                     "config":
                         {"server": backend_url
                          }
                     },
            }

            with open(f'{folder_name}/recipe.yml', 'w+') as f:
                documents = yaml.dump(raw_config, f)
            print("START OF OPEN API PROCESSING################################################################")
            count += 1
            print("Folder with recipe.yml: " + folder_name + f" Sequence number of loaded open API: {count}")
            logger.info(
                "Folder with recipe.yml: " + folder_name + f" Sequence number of loaded open API:: {count}"
            )
            print("OpenAPI name: " + api_name)
            logger.info(
                "OpenAPI name: " + api_name
            )
            os.system('datahub  ingest run -c /tmp/datahub/ingest/' + folder_unique_name + '/recipe.yml')
            print("END OF PROCESSING############################################################################")

        self.report.report_warning(key="backstage", reason="Backstage pipeline does not produce own datasets.")
        self.report.report_warning(key="backstage", reason=f"Backstage pipeline processed {count} OpenAPI pipelines.")

        yield from self.build_wu("", "backstage")

    def get_report(self):
        return self.report


class MyBackstageSource(BackstageSource):
    def __init__(self, config: BackstageConfig, ctx: PipelineContext):
        super().__init__(config, ctx, "Backstage")

    @classmethod
    def create(cls, config_dict, ctx):
        config = BackstageConfig.parse_obj(config_dict)
        return cls(config, ctx)

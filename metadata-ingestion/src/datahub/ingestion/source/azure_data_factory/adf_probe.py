from typing import Dict, List

from datahub.ingestion.agent.probe_methods import probe_method
from datahub.ingestion.source.azure_data_factory.adf_client import (
    AzureDataFactoryClient,
)
from datahub.ingestion.source.azure_data_factory.adf_config import (
    AzureDataFactoryConfig,
)
from datahub.ingestion.source.azure_data_factory.adf_report import (
    AzureDataFactorySourceReport,
)
from datahub.ingestion.source.azure_data_factory.adf_source import (
    AzureDataFactorySource,
)
from datahub.ingestion.source.common.subtypes import FlowContainerSubTypes


class AzureDataFactoryMetadataProbe:
    """Metadata-only probe over Azure Data Factory's management API.

    Every record is an allowlist projection of the SDK model. ADF keeps
    credentials inside the objects this lists -- a linked service's
    connection string, an HTTP dataset's headers, a Web activity's auth, a
    factory's global parameters -- and register_secrets only masks secrets
    that came from the recipe, so nothing else would catch them.

    Reuses the connector's fetch, never its policy: no getter applies
    factory_pattern or pipeline_pattern, so a denied name is reported for
    `probe filter` to explain rather than hidden.
    """

    warnings: List[str]

    def __init__(self, source: AzureDataFactorySource, credential: object) -> None:
        self._source = source
        self._config: AzureDataFactoryConfig = source.config
        self._client: AzureDataFactoryClient = source.client
        self._credential = credential
        self.warnings = []

    @classmethod
    def for_config(
        cls, config: AzureDataFactoryConfig
    ) -> "AzureDataFactoryMetadataProbe":
        # The two calls AzureDataFactorySource.__init__ makes, so auth resolves
        # exactly as ingestion resolves it. Neither contacts Azure: tokens are
        # acquired on the first request.
        credential = config.credential.get_credential()
        client = AzureDataFactoryClient(
            credential=credential, subscription_id=config.subscription_id
        )
        return cls(AzureDataFactorySource.for_probe(config, client), credential)

    def __enter__(self) -> "AzureDataFactoryMetadataProbe":
        return self

    def __exit__(self, *exc: object) -> None:
        try:
            self._client.close()
        finally:
            # Ingestion never closes the credential; a probe is a short-lived
            # process, and closing it releases the token client's transport.
            close = getattr(self._credential, "close", None)
            if callable(close):
                close()

    @property
    def probe_report(self) -> AzureDataFactorySourceReport:
        """_resolve_dataset_urn records unmapped platforms here with
        report.warning(); exposing it folds those into the result."""
        return self._source.report

    def _warn(self, message: str) -> None:
        if message not in self.warnings:
            self.warnings.append(message)

    @probe_method(kind=FlowContainerSubTypes.ADF_DATA_FACTORY, row_limit_param="limit")
    def factories(self, limit: int = 200) -> List[Dict[str, object]]:
        """Data Factories in this subscription, including ones factory_pattern
        would exclude -- a denied factory is reported, not hidden, so `probe
        filter --kind "Data Factory"` can explain it. When the recipe sets
        resource_group, only that resource group is listed, as ingestion lists
        it. Each record is name, resource group and region; factory global
        parameters are withheld because their values can be secrets."""
        # Not soft: a failure here means nothing could be read, which is exit 3.
        rg = self._config.resource_group
        out: List[Dict[str, object]] = []
        nameless = 0
        for factory in self._client.get_factories(resource_group=rg):
            if not factory.name or not factory.id:
                nameless += 1
                continue
            out.append(
                {
                    "name": factory.name,
                    "resource_group": self._source._extract_resource_group(
                        factory.id
                    ),
                    "location": factory.location,
                }
            )
            if len(out) >= limit:
                break
        if nameless:
            self._warn(
                f"{nameless} factor{'y' if nameless == 1 else 'ies'} had no name "
                f"or id and were skipped, as ingestion skips them"
            )
        if rg:
            self._warn(
                f"resource_group is set to '{rg}', so only that resource group "
                f"was listed; factories elsewhere in the subscription are absent "
                f"rather than reported excluded, and ingestion will not see them"
            )
        return out

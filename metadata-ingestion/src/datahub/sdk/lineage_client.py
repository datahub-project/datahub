from __future__ import annotations

from typing import TYPE_CHECKING, Literal, Optional, Union

import datahub.metadata.schema_classes as models
from datahub.errors import SdkUsageError
from datahub.metadata.urns import DatasetUrn
from datahub.sdk._shared import DatajobUrnOrStr, DatasetUrnOrStr
from datahub.sdk.dataset import ColumnLineageMapping, _parse_cll_mapping
from datahub.specific.dataset import DatasetPatchBuilder

if TYPE_CHECKING:
    from datahub.sdk.main_client import DataHubClient


class LineageClient:
    def __init__(self, client: DataHubClient):
        self._client = client

    def add_dataset_copy_lineage(
        self,
        *,
        upstream: DatasetUrnOrStr,
        downstream: DatasetUrnOrStr,
        column_lineage: Union[
            None, ColumnLineageMapping, Literal["fuzzy_auto", "strict_auto"]
        ] = "fuzzy_auto",
    ) -> None:
        # TODO: Add support for the auto lineage mapping.
        # This should be a more advanced, fuzzy match based on the column names.
        raise NotImplementedError("TODO")

    def add_dataset_transform_lineage(
        self,
        *,
        upstream: DatasetUrnOrStr,
        downstream: DatasetUrnOrStr,
        column_lineage: Optional[ColumnLineageMapping] = None,
        query_text: Optional[str] = None,
        query_lang: str = "sql",
    ) -> None:
        upstream = DatasetUrn.from_string(upstream)
        downstream = DatasetUrn.from_string(downstream)

        cll = None
        if column_lineage is not None:
            cll = _parse_cll_mapping(
                upstream=upstream,
                downstream=downstream,
                cll_mapping=column_lineage,
            )

        # TODO: Create a query entity + link to it from the lineage class.

        updater = DatasetPatchBuilder(str(downstream))
        updater.add_upstream_lineage(
            models.UpstreamClass(
                dataset=str(upstream),
                type=models.DatasetLineageTypeClass.TRANSFORMED,
            )
        )
        for cl in cll or []:
            updater.add_fine_grained_upstream_lineage(cl)

        # Will throw if the dataset does not exist.
        self._client.update(updater)

    def add_datajob_lineage(
        self,
        *,
        job: DatajobUrnOrStr,
        upstream: Optional[DatasetUrnOrStr],
        downstream: Optional[DatasetUrnOrStr],
        column_lineage: ColumnLineageMapping,
    ) -> None:
        upstream = DatasetUrn.from_string(upstream) if upstream else None
        downstream = DatasetUrn.from_string(downstream) if downstream else None

        if not upstream and not downstream:
            raise SdkUsageError(
                "upstream or downstream is required for datajob lineage"
            )

        cll = None
        if column_lineage is not None:
            # upstream and downstream are required
            if not upstream or not downstream:
                raise SdkUsageError(
                    "upstream and downstream are required for column lineage"
                )
            cll = _parse_cll_mapping(
                upstream=upstream,
                downstream=downstream,
                cll_mapping=column_lineage,
            )

        assert cll or True

        raise NotImplementedError("TODO")

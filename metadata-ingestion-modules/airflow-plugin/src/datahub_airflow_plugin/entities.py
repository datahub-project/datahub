from abc import abstractmethod
from typing import List, Optional

import attr

import datahub.emitter.mce_builder as builder
from datahub.utilities.urns.data_job_urn import DataJobUrn
from datahub.utilities.urns.dataset_urn import DatasetUrn
from datahub.utilities.urns.urn import guess_entity_type


class _Entity:
    @property
    @abstractmethod
    def urn(self) -> str:
        pass


@attr.s(auto_attribs=True, str=True)
class Dataset(_Entity):
    platform: str
    name: str
    env: str = builder.DEFAULT_ENV
    platform_instance: Optional[str] = None

    @property
    def urn(self):
        return builder.make_dataset_urn_with_platform_instance(
            platform=self.platform,
            name=self.name,
            platform_instance=self.platform_instance,
            env=self.env,
        )


@attr.s(str=True)
class Urn(_Entity):
    _urn: str = attr.ib()

    @_urn.validator
    def _validate_urn(self, attribute, value):
        if not value.startswith("urn:"):
            raise ValueError("invalid urn provided: urns must start with 'urn:'")
        if guess_entity_type(value) not in ["dataset", "dataJob"]:
            # This is because DataJobs only support Dataset and upstream Datajob lineage.
            raise ValueError(
                "Airflow lineage currently only supports datasets and upstream datajobs"
            )

    @property
    def urn(self):
        return self._urn


def entities_to_dataset_urn_list(iolets: List[str]) -> List[DatasetUrn]:
    dataset_urn_list: List[DatasetUrn] = []
    for let in iolets:
        if guess_entity_type(let) == "dataset":
            dataset_urn_list.append(DatasetUrn.from_string(let))
    return dataset_urn_list


def entities_to_datajob_urn_list(inlets: List[str]) -> List[DataJobUrn]:
    datajob_urn_list: List[DataJobUrn] = []
    for let in inlets:
        if guess_entity_type(let) == "dataJob":
            datajob_urn_list.append(DataJobUrn.from_string(let))
    return datajob_urn_list

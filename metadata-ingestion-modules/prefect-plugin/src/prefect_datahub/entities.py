from abc import abstractmethod
from typing import Optional

import attr

import datahub.emitter.mce_builder as builder
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
        if guess_entity_type(value) != "dataset":
            raise ValueError("Datajob input/output currently only supports datasets")

    @property
    def urn(self):
        return self._urn

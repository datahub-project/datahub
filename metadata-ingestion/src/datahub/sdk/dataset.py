from typing import List, Optional, Type

import datahub.metadata.schema_classes as models
from datahub.emitter.mce_builder import DEFAULT_ENV
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.urns import DatasetUrn, Urn
from datahub.sdk._shared import Entity, HasSubtype, HasUrn, OwnersInputType, UrnOrStr
from datahub.specific.dataset import DatasetPatchBuilder


class DatasetUpdater:
    # basically a clone of the DatasetPatchBuilder, but with a few more methods

    DatasetPatchBuilder
    pass


def set_owners(self: Entity, owners: List[models.OwnerClass], /) -> None:
    """Set the owners of the entity."""
    self._set_aspect(models.OwnershipClass(owners=owners))


class Dataset(HasSubtype, Entity):
    @classmethod
    def get_urn_type(cls) -> Type[Urn]:
        return DatasetUrn

    def __init__(
        self,
        *,
        # Identity.
        platform: str,
        name: str,
        platform_instance: Optional[str] = None,
        env: str = DEFAULT_ENV,
        # TODO have an urn-based variant? probably not, since we need to know the raw platform instance
        # Attributes.
        # description
        # external_url
        subtype: Optional[str] = None,
        owners: Optional[OwnersInputType] = None,
        # owners
        # tags
        # terms
        # structured_properties
        # TODO: schema -> how do we make this feel nice
        # TODO container / browse path generation?
    ):
        urn = DatasetUrn.create_from_ids(
            platform_id=platform,
            table_name=name,
            platform_instance=platform_instance,
            env=env,
        )
        super().__init__(urn)

        # TODO:
        if platform_instance is not None:
            # force create dataPlatformInstance aspect
            pass

        if subtype is not None:
            self.subtype = subtype

        if owners is not None:
            self.owners = owners

    @property
    def urn(self) -> DatasetUrn:
        return self._urn  # type: ignore

    @property
    def platform_instance(self) -> Optional[str]:
        dataPlatformInstance = self._get_aspect(models.DataPlatformInstanceClass)
        if dataPlatformInstance and dataPlatformInstance.instance:
            return dataPlatformInstance.instance
        return None

    set_owners = set_owners


def graph_get_dataset(graph: DataHubGraph, urn: UrnOrStr) -> Dataset:
    if not isinstance(urn, Urn):
        urn = Urn.from_string(urn)

    assert isinstance(urn, DatasetUrn)

    aspects = graph.get_entity_semityped(str(urn))

    # TODO get the right entity type subclass
    return Dataset._new_from_graph(urn, aspects)


if __name__ == "__main__":
    d = Dataset(
        platform="bigquery",
        name="test",
    )
    print(d.urn)

    # assert issubclass(Entity, HasUrn)
    # assert issubclass(Dataset, HasUrn)
    assert isinstance(d, HasUrn)
    # assert issubclass(DatasetPatchBuilder, HasUrn)

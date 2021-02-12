import { Dataset, EntityType, FabricType, OwnershipType } from '../../../../../types.generated';

export const sampleDataset: Dataset = {
    __typename: 'Dataset',
    urn: 'test:urn',
    platform: {
        type: EntityType.DataPlatform,
        urn: 'test:hive:urn',
        name: 'hive',
    },
    name: 'hive dataset',
    origin: FabricType.Prod,
    description: 'Some description',
    type: EntityType.Dataset,
    created: { time: 1 },
    lastModified: { time: 1 },
    tags: [],
    ownership: {
        owners: [
            { owner: { urn: 'user:urn', type: EntityType.CorpUser, username: 'UserA' }, type: OwnershipType.Dataowner },
        ],
        lastModified: { time: 1 },
    },
};

export const sampleDeprecatedDataset: Dataset = {
    __typename: 'Dataset',
    urn: 'test:urn',
    platform: {
        type: EntityType.DataPlatform,
        urn: 'test:hive:urn',
        name: 'hive',
    },
    name: 'hive dataset',
    origin: FabricType.Prod,
    description: 'Some deprecated description',
    type: EntityType.Dataset,
    created: { time: 1 },
    lastModified: { time: 1 },
    tags: [],
    ownership: {
        owners: [
            { owner: { urn: 'user:urn', type: EntityType.CorpUser, username: 'UserA' }, type: OwnershipType.Dataowner },
        ],
        lastModified: { time: 1 },
    },
    deprecation: {
        actor: 'UserB',
        deprecated: true,
        note: "Don't touch this dataset with a 10 foot pole",
        decommissionTime: 1612565520292,
    },
};

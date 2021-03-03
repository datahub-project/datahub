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
    tags: [],
    ownership: {
        owners: [
            { owner: { urn: 'user:urn', type: EntityType.CorpUser, username: 'UserA' }, type: OwnershipType.Dataowner },
        ],
        lastModified: { time: 1 },
    },
    globalTags: null,
    upstreamLineage: null,
    downstreamLineage: null,
    institutionalMemory: {
        elements: [
            {
                url: 'https://www.google.com',
                author: 'datahub',
                description: 'This only points to Google',
                created: {
                    actor: 'urn:li:corpuser:1',
                    time: 1612396473001,
                },
            },
        ],
    },
    schema: null,
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
    globalTags: null,
    upstreamLineage: null,
    downstreamLineage: null,
    institutionalMemory: {
        elements: [
            {
                url: 'https://www.google.com',
                author: 'datahub',
                description: 'This only points to Google',
                created: {
                    actor: 'urn:li:corpuser:1',
                    time: 1612396473001,
                },
            },
        ],
    },
    schema: null,
};

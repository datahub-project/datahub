import { EntityType, FabricType, PlatformNativeType } from '../../../../../types.generated';

export const sampleUpstreamEntities = [
    {
        name: 'Upstream HiveDataset',
        type: EntityType.Dataset,
        urn: 'abc',
        platform: {
            urn: 'urn:li:dataPlatform:hive',
            name: 'Hive',
            type: EntityType.DataPlatform,
        },
        origin: FabricType.Prod,
        description: 'this is a dataset',
        platformNativeType: PlatformNativeType.Table,
        tags: [],
        created: {
            time: 0,
        },
        lastModified: {
            time: 0,
        },
    },
    {
        name: 'Upstream KafkaDataset',
        type: EntityType.Dataset,
        urn: 'abc',
        platform: {
            urn: 'urn:li:dataPlatform:hive',
            name: 'Hive',
            type: EntityType.DataPlatform,
        },
        origin: FabricType.Prod,
        description: 'this is a dataset',
        platformNativeType: PlatformNativeType.Table,
        tags: [],
        created: {
            time: 0,
        },
        lastModified: {
            time: 0,
        },
    },
];

export const sampleDownstreamEntities = [
    {
        name: 'Downstream HiveDataset',
        type: EntityType.Dataset,
        urn: 'abc',
        platform: {
            urn: 'urn:li:dataPlatform:hive',
            name: 'Hive',
            type: EntityType.DataPlatform,
        },
        origin: FabricType.Prod,
        description: 'this is a dataset',
        platformNativeType: PlatformNativeType.Table,
        tags: [],
        created: {
            time: 0,
        },
        lastModified: {
            time: 0,
        },
    },
    {
        name: 'Downstream KafkaDataset',
        type: EntityType.Dataset,
        urn: 'abc',
        platform: {
            urn: 'urn:li:dataPlatform:hive',
            name: 'Hive',
            type: EntityType.DataPlatform,
        },
        origin: FabricType.Prod,
        description: 'this is a dataset',
        platformNativeType: PlatformNativeType.Table,
        tags: [],
        created: {
            time: 0,
        },
        lastModified: {
            time: 0,
        },
    },
];

export const sampleRelationship = {
    entities: sampleUpstreamEntities.map((entity) => ({
        entity,
        created: { time: 0 },
    })),
};

export const sampleDownstreamRelationship = {
    entities: sampleDownstreamEntities.map((entity) => ({
        entity,
        created: { time: 0 },
    })),
};

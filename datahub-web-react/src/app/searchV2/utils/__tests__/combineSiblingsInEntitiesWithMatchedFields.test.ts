import {
    EntityWithMatchedFields,
    combineSiblingsInEntitiesWithMatchedFields,
} from '@app/searchV2/utils/combineSiblingsInEntitiesWithMatchedFields';
import { Dataset, Entity, EntityType, FabricType, MatchedField } from '@src/types.generated';

function generateSampleEntity(urn: string, platformUrn: string, isPrimary: boolean, siblings: Entity[]): Dataset {
    return {
        urn,
        type: EntityType.Dataset,
        name: urn,
        exists: true,
        platform: {
            name: platformUrn,
            type: EntityType.DataPlatform,
            urn: platformUrn,
        },
        origin: FabricType.Test,
        siblings: {
            isPrimary,
            siblings,
        },
        siblingsSearch: {
            count: siblings?.length || 0,
            total: siblings?.length || 0,
            searchResults:
                siblings?.map((sibling) => ({
                    entity: sibling,
                    matchedFields: [],
                })) || [],
        },
    };
}

function wrapWithMatchedFields(entity: Entity, matchedFields: MatchedField[] = []): EntityWithMatchedFields {
    return {
        entity,
        matchedFields,
    };
}

describe('combineSiblingsInEntitiesWithMatchedFields', () => {
    it('should return an empty array when input entities are undefined', () => {
        const response = combineSiblingsInEntitiesWithMatchedFields(undefined, true);
        expect(response).toStrictEqual([]);
    });

    it('should return an empty array when input entities are empty', () => {
        const response = combineSiblingsInEntitiesWithMatchedFields([], true);
        expect(response).toStrictEqual([]);
    });

    it('should return entities unchanged when shouldSepareteSiblings is false', () => {
        const sample1 = generateSampleEntity('test1', 'platform1', false, []);
        const sample2 = generateSampleEntity('test2', 'platform2', false, []);

        const response = combineSiblingsInEntitiesWithMatchedFields(
            [wrapWithMatchedFields(sample1), wrapWithMatchedFields(sample2)],
            false,
        );

        expect(response).toStrictEqual([wrapWithMatchedFields(sample1), wrapWithMatchedFields(sample2)]);
    });

    it('should handle entities without siblings correctly when shouldSepareteSiblings is true', () => {
        const sample1 = generateSampleEntity('test1', 'platform1', false, []);
        const sample2 = generateSampleEntity('test2', 'platform2', false, []);

        const response = combineSiblingsInEntitiesWithMatchedFields(
            [wrapWithMatchedFields(sample1), wrapWithMatchedFields(sample2)],
            true,
        );

        expect(response).toStrictEqual([wrapWithMatchedFields(sample1), wrapWithMatchedFields(sample2)]);
    });

    it('should combine entities with siblings when shouldSepareteSiblings is true', () => {
        const sample1 = generateSampleEntity('test1', 'platform1', false, []);
        const sample2 = generateSampleEntity('test2', 'platform2', false, [sample1]);

        const response = combineSiblingsInEntitiesWithMatchedFields(
            [wrapWithMatchedFields(sample2, [{ name: 'test', value: 'testValue' }]), wrapWithMatchedFields(sample1)],
            true,
        );

        expect(response).toStrictEqual([
            {
                entity: {
                    ...sample2,
                    properties: {
                        externalUrl: undefined,
                    },
                },
                matchedEntities: [
                    sample1,
                    {
                        ...sample2,
                        properties: {
                            externalUrl: undefined,
                        },
                        siblingPlatforms: null,
                        siblings: null,
                        siblingsSearch: null,
                    },
                ],
                matchedFields: [
                    {
                        name: 'test',
                        value: 'testValue',
                    },
                ],
            },
        ]);
    });

    it('should handle multiple entities with complex sibling relationships', () => {
        const sample1 = generateSampleEntity('test1', 'platform1', false, []);
        const sample2 = generateSampleEntity('test2', 'platform2', false, [sample1]);
        const sample3 = generateSampleEntity('test3', 'platform3', false, [sample1, sample2]);

        const response = combineSiblingsInEntitiesWithMatchedFields(
            [wrapWithMatchedFields(sample3), wrapWithMatchedFields(sample2), wrapWithMatchedFields(sample1)],
            true,
        );

        expect(response).toStrictEqual([
            {
                entity: {
                    ...sample3,
                    properties: {
                        externalUrl: undefined,
                    },
                },
                matchedEntities: [
                    sample1,
                    sample2,
                    {
                        ...sample3,
                        properties: {
                            externalUrl: undefined,
                        },
                        siblingPlatforms: null,
                        siblings: null,
                        siblingsSearch: null,
                    },
                ],
                matchedFields: [],
            },
        ]);
    });
});

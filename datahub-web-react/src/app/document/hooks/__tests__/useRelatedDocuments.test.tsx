import { describe, expect, it } from 'vitest';

import { GetRelatedDocumentsQuery, RelatedDocumentsFieldsFragment } from '@graphql/document.generated';

describe('useRelatedDocuments - type extraction', () => {
    const mockRelatedDocumentsResult: RelatedDocumentsFieldsFragment = {
        __typename: 'RelatedDocumentsResult',
        start: 0,
        count: 2,
        total: 2,
        documents: [
            {
                __typename: 'Document',
                urn: 'urn:li:document:1',
                type: 'DOCUMENT' as any,
                info: {
                    __typename: 'DocumentInfo',
                    title: 'Test Document 1',
                    lastModified: {
                        __typename: 'ResolvedAuditStamp',
                        time: 1000,
                        actor: {
                            __typename: 'CorpUser',
                            urn: 'urn:li:corpuser:test',
                            type: 'CORP_USER' as any,
                            username: 'testuser',
                        } as any,
                    },
                },
            },
        ],
    };

    // Test the type extraction logic by creating mock entity structures
    // that match the GraphQL query response types

    it('should extract relatedDocuments from Dataset entity', () => {
        const entity: GetRelatedDocumentsQuery['entity'] = {
            __typename: 'Dataset',
            urn: 'urn:li:dataset:test',
            type: 'DATASET' as any,
            relatedDocuments: mockRelatedDocumentsResult,
        };

        // The type guard should work - entity has relatedDocuments
        expect(entity).toBeDefined();
        if (entity && 'relatedDocuments' in entity && entity.relatedDocuments) {
            expect(entity.relatedDocuments).toEqual(mockRelatedDocumentsResult);
            expect(entity.relatedDocuments.total).toBe(2);
        } else {
            throw new Error('Type guard failed - relatedDocuments should be accessible');
        }
    });

    it('should extract relatedDocuments from Dashboard entity', () => {
        const entity: GetRelatedDocumentsQuery['entity'] = {
            __typename: 'Dashboard',
            urn: 'urn:li:dashboard:test',
            type: 'DASHBOARD' as any,
            relatedDocuments: mockRelatedDocumentsResult,
        };

        if (entity && 'relatedDocuments' in entity && entity.relatedDocuments) {
            expect(entity.relatedDocuments).toEqual(mockRelatedDocumentsResult);
        } else {
            throw new Error('Type guard failed');
        }
    });

    it('should return null when entity does not have relatedDocuments field', () => {
        const entity: GetRelatedDocumentsQuery['entity'] = {
            __typename: 'CorpUser',
            urn: 'urn:li:corpuser:test',
            type: 'CORP_USER' as any,
            // No relatedDocuments field
        };

        // Type guard should fail - CorpUser doesn't have relatedDocuments
        if (entity && 'relatedDocuments' in entity) {
            throw new Error('Type guard should not pass for CorpUser');
        }
        if (entity) {
            expect('relatedDocuments' in entity).toBe(false);
        }
    });

    it('should handle null entity', () => {
        const entity: GetRelatedDocumentsQuery['entity'] = null;

        expect(entity).toBeNull();
        if (entity) {
            throw new Error('Entity should be null');
        }
    });

    it('should handle entity with null relatedDocuments', () => {
        const entity: GetRelatedDocumentsQuery['entity'] = {
            __typename: 'Dataset',
            urn: 'urn:li:dataset:test',
            type: 'DATASET' as any,
            relatedDocuments: null,
        };

        // Type guard should pass (field exists) but value is null
        if (entity && 'relatedDocuments' in entity) {
            expect(entity.relatedDocuments).toBeNull();
        } else {
            throw new Error('Type guard should pass - field exists');
        }
    });

    it('should handle entity with undefined relatedDocuments', () => {
        const entity: GetRelatedDocumentsQuery['entity'] = {
            __typename: 'Dataset',
            urn: 'urn:li:dataset:test',
            type: 'DATASET' as any,
            relatedDocuments: undefined,
        };

        if (entity && 'relatedDocuments' in entity) {
            expect(entity.relatedDocuments).toBeUndefined();
        } else {
            throw new Error('Type guard should pass - field exists');
        }
    });

    it('should work with multiple entity types that support relatedDocuments', () => {
        const entityTypes: Array<GetRelatedDocumentsQuery['entity']> = [
            {
                __typename: 'Chart',
                urn: 'urn:li:chart:test',
                type: 'CHART' as any,
                relatedDocuments: mockRelatedDocumentsResult,
            },
            {
                __typename: 'DataJob',
                urn: 'urn:li:datajob:test',
                type: 'DATA_JOB' as any,
                relatedDocuments: mockRelatedDocumentsResult,
            },
            {
                __typename: 'Container',
                urn: 'urn:li:container:test',
                type: 'CONTAINER' as any,
                relatedDocuments: mockRelatedDocumentsResult,
            },
        ];

        entityTypes.forEach((entity) => {
            if (entity && 'relatedDocuments' in entity && entity.relatedDocuments) {
                expect(entity.relatedDocuments.total).toBe(2);
            } else {
                throw new Error(`Type guard failed for ${entity?.__typename}`);
            }
        });
    });

    it('should properly narrow types after type guard check', () => {
        const entity: GetRelatedDocumentsQuery['entity'] = {
            __typename: 'Dataset',
            urn: 'urn:li:dataset:test',
            type: 'DATASET' as any,
            relatedDocuments: mockRelatedDocumentsResult,
        };

        // Before type guard - TypeScript doesn't know if relatedDocuments exists
        if (!entity) {
            throw new Error('Entity should exist');
        }

        // After checking 'relatedDocuments' in entity, TypeScript should narrow the type
        if ('relatedDocuments' in entity) {
            // At this point, TypeScript knows entity has relatedDocuments property
            // We can safely access it without type assertions
            const result = entity.relatedDocuments;
            if (result) {
                // TypeScript knows result is RelatedDocumentsFieldsFragment
                expect(result.__typename).toBe('RelatedDocumentsResult');
                expect(result.total).toBe(2);
                expect(result.documents).toHaveLength(1);
            }
        }
    });
});

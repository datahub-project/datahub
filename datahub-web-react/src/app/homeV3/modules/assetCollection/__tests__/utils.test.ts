import { sortByUrnOrder } from '@app/homeV3/modules/assetCollection/utils';

// Mock entity interface with urn property
interface MockEntity {
    urn: string;
    name?: string;
    id?: number;
}

describe('sortByUrnOrder', () => {
    describe('normal sorting behavior', () => {
        it('should sort objects according to the provided urn order', () => {
            const entities: MockEntity[] = [
                { urn: 'urn:li:dataset:3', name: 'Third Dataset' },
                { urn: 'urn:li:dataset:1', name: 'First Dataset' },
                { urn: 'urn:li:dataset:2', name: 'Second Dataset' },
            ];

            const orderedUrns = ['urn:li:dataset:1', 'urn:li:dataset:2', 'urn:li:dataset:3'];

            const result = sortByUrnOrder(entities, orderedUrns);

            expect(result).toEqual([
                { urn: 'urn:li:dataset:1', name: 'First Dataset' },
                { urn: 'urn:li:dataset:2', name: 'Second Dataset' },
                { urn: 'urn:li:dataset:3', name: 'Third Dataset' },
            ]);
        });

        it('should maintain original array order for objects with same urn order index', () => {
            const entities: MockEntity[] = [
                { urn: 'urn:li:dataset:1', name: 'Dataset A', id: 1 },
                { urn: 'urn:li:dataset:1', name: 'Dataset B', id: 2 },
            ];

            const orderedUrns = ['urn:li:dataset:1'];

            const result = sortByUrnOrder(entities, orderedUrns);

            // Should maintain original order for same URN
            expect(result[0].id).toBe(1);
            expect(result[1].id).toBe(2);
        });

        it('should work with different entity types', () => {
            interface DataProduct {
                urn: string;
                displayName: string;
                type: string;
            }

            const dataProducts: DataProduct[] = [
                { urn: 'urn:li:dataProduct:analytics', displayName: 'Analytics', type: 'dataProduct' },
                { urn: 'urn:li:dataProduct:customer', displayName: 'Customer', type: 'dataProduct' },
                { urn: 'urn:li:dataProduct:billing', displayName: 'Billing', type: 'dataProduct' },
            ];

            const orderedUrns = [
                'urn:li:dataProduct:customer',
                'urn:li:dataProduct:billing',
                'urn:li:dataProduct:analytics',
            ];

            const result = sortByUrnOrder(dataProducts, orderedUrns);

            expect(result.map((dp) => dp.displayName)).toEqual(['Customer', 'Billing', 'Analytics']);
        });
    });

    describe('edge cases with missing URNs', () => {
        it('should place objects with URNs not in ordered list at the end', () => {
            const entities: MockEntity[] = [
                { urn: 'urn:li:dataset:unknown', name: 'Unknown Dataset' },
                { urn: 'urn:li:dataset:1', name: 'First Dataset' },
                { urn: 'urn:li:dataset:also-unknown', name: 'Also Unknown' },
                { urn: 'urn:li:dataset:2', name: 'Second Dataset' },
            ];

            const orderedUrns = ['urn:li:dataset:1', 'urn:li:dataset:2'];

            const result = sortByUrnOrder(entities, orderedUrns);

            // First two should be ordered according to orderedUrns
            expect(result[0]).toEqual({ urn: 'urn:li:dataset:1', name: 'First Dataset' });
            expect(result[1]).toEqual({ urn: 'urn:li:dataset:2', name: 'Second Dataset' });

            // Last two should be the unknown ones (maintaining their relative order)
            expect(result[2]).toEqual({ urn: 'urn:li:dataset:unknown', name: 'Unknown Dataset' });
            expect(result[3]).toEqual({ urn: 'urn:li:dataset:also-unknown', name: 'Also Unknown' });
        });

        it('should handle case where no URNs match the ordered list', () => {
            const entities: MockEntity[] = [
                { urn: 'urn:li:dataset:x', name: 'X' },
                { urn: 'urn:li:dataset:y', name: 'Y' },
                { urn: 'urn:li:dataset:z', name: 'Z' },
            ];

            const orderedUrns = ['urn:li:dataset:1', 'urn:li:dataset:2'];

            const result = sortByUrnOrder(entities, orderedUrns);

            // Should maintain original order since no URNs match
            expect(result).toEqual(entities);
        });

        it('should handle case where ordered URNs list contains URNs not in objects', () => {
            const entities: MockEntity[] = [
                { urn: 'urn:li:dataset:2', name: 'Second' },
                { urn: 'urn:li:dataset:1', name: 'First' },
            ];

            const orderedUrns = [
                'urn:li:dataset:1',
                'urn:li:dataset:2',
                'urn:li:dataset:3', // This URN doesn't exist in entities
                'urn:li:dataset:4', // This URN doesn't exist in entities
            ];

            const result = sortByUrnOrder(entities, orderedUrns);

            expect(result).toEqual([
                { urn: 'urn:li:dataset:1', name: 'First' },
                { urn: 'urn:li:dataset:2', name: 'Second' },
            ]);
        });
    });

    describe('empty arrays and edge cases', () => {
        it('should handle empty objects array', () => {
            const entities: MockEntity[] = [];
            const orderedUrns = ['urn:li:dataset:1', 'urn:li:dataset:2'];

            const result = sortByUrnOrder(entities, orderedUrns);

            expect(result).toEqual([]);
        });

        it('should handle empty ordered URNs array', () => {
            const entities: MockEntity[] = [
                { urn: 'urn:li:dataset:2', name: 'Second' },
                { urn: 'urn:li:dataset:1', name: 'First' },
            ];
            const orderedUrns: string[] = [];

            const result = sortByUrnOrder(entities, orderedUrns);

            // Should maintain original order when no ordering is provided
            expect(result).toEqual(entities);
        });

        it('should handle both arrays being empty', () => {
            const entities: MockEntity[] = [];
            const orderedUrns: string[] = [];

            const result = sortByUrnOrder(entities, orderedUrns);

            expect(result).toEqual([]);
        });

        it('should handle single object', () => {
            const entities: MockEntity[] = [{ urn: 'urn:li:dataset:1', name: 'Only Dataset' }];
            const orderedUrns = ['urn:li:dataset:1'];

            const result = sortByUrnOrder(entities, orderedUrns);

            expect(result).toEqual(entities);
        });
    });

    describe('mixed scenarios', () => {
        it('should handle partial matches with complex sorting', () => {
            const entities: MockEntity[] = [
                { urn: 'urn:li:dataset:unknown1', name: 'Unknown 1' },
                { urn: 'urn:li:dataset:c', name: 'C' },
                { urn: 'urn:li:dataset:unknown2', name: 'Unknown 2' },
                { urn: 'urn:li:dataset:a', name: 'A' },
                { urn: 'urn:li:dataset:b', name: 'B' },
                { urn: 'urn:li:dataset:unknown3', name: 'Unknown 3' },
            ];

            const orderedUrns = ['urn:li:dataset:a', 'urn:li:dataset:b', 'urn:li:dataset:c'];

            const result = sortByUrnOrder(entities, orderedUrns);

            // First three should be ordered a, b, c
            expect(result[0].name).toBe('A');
            expect(result[1].name).toBe('B');
            expect(result[2].name).toBe('C');

            // Rest should be unknowns in original order
            expect(result[3].name).toBe('Unknown 1');
            expect(result[4].name).toBe('Unknown 2');
            expect(result[5].name).toBe('Unknown 3');
        });

        it('should not mutate the original arrays', () => {
            const originalEntities: MockEntity[] = [
                { urn: 'urn:li:dataset:3', name: 'Third' },
                { urn: 'urn:li:dataset:1', name: 'First' },
                { urn: 'urn:li:dataset:2', name: 'Second' },
            ];

            const originalOrderedUrns = ['urn:li:dataset:1', 'urn:li:dataset:2', 'urn:li:dataset:3'];

            // Create copies to compare against
            const entitiesCopy = [...originalEntities];
            const urnsCopy = [...originalOrderedUrns];

            const result = sortByUrnOrder(originalEntities, originalOrderedUrns);

            // Original arrays should remain unchanged
            expect(originalEntities).toEqual(entitiesCopy);
            expect(originalOrderedUrns).toEqual(urnsCopy);

            // Result should be different from original order
            expect(result).not.toEqual(originalEntities);
            expect(result[0].name).toBe('First');
        });

        it('should work correctly with duplicate URNs in ordered list', () => {
            const entities: MockEntity[] = [
                { urn: 'urn:li:dataset:1', name: 'First', id: 1 },
                { urn: 'urn:li:dataset:2', name: 'Second', id: 2 },
            ];

            // Ordered URNs with duplicate
            const orderedUrns = ['urn:li:dataset:2', 'urn:li:dataset:1', 'urn:li:dataset:2'];

            const result = sortByUrnOrder(entities, orderedUrns);

            // Should use the first occurrence index (index 0 for dataset:2, index 1 for dataset:1)
            expect(result[0].name).toBe('Second');
            expect(result[1].name).toBe('First');
        });
    });

    describe('performance and data integrity', () => {
        it('should handle large datasets efficiently', () => {
            // Create a large dataset
            const largeEntityList: MockEntity[] = Array.from({ length: 1000 }, (_, i) => ({
                urn: `urn:li:dataset:${i}`,
                name: `Dataset ${i}`,
            }));

            // Create ordered URNs in reverse order
            const orderedUrns = Array.from({ length: 1000 }, (_, i) => `urn:li:dataset:${999 - i}`);

            const startTime = performance.now();
            const result = sortByUrnOrder(largeEntityList, orderedUrns);
            const endTime = performance.now();

            // Should complete in reasonable time (less than 100ms)
            expect(endTime - startTime).toBeLessThan(100);

            // Verify correct sorting
            expect(result[0].name).toBe('Dataset 999');
            expect(result[999].name).toBe('Dataset 0');
            expect(result).toHaveLength(1000);
        });

        it('should preserve all object properties during sorting', () => {
            interface ComplexEntity {
                urn: string;
                name: string;
                metadata: {
                    created: Date;
                    tags: string[];
                };
                nestedObject: {
                    deep: {
                        value: number;
                    };
                };
            }

            const complexEntities: ComplexEntity[] = [
                {
                    urn: 'urn:li:dataset:2',
                    name: 'Second',
                    metadata: {
                        created: new Date('2023-01-02'),
                        tags: ['tag1', 'tag2'],
                    },
                    nestedObject: {
                        deep: {
                            value: 42,
                        },
                    },
                },
                {
                    urn: 'urn:li:dataset:1',
                    name: 'First',
                    metadata: {
                        created: new Date('2023-01-01'),
                        tags: ['tag3'],
                    },
                    nestedObject: {
                        deep: {
                            value: 24,
                        },
                    },
                },
            ];

            const orderedUrns = ['urn:li:dataset:1', 'urn:li:dataset:2'];

            const result = sortByUrnOrder(complexEntities, orderedUrns);

            // Verify all properties are preserved
            expect(result[0]).toEqual({
                urn: 'urn:li:dataset:1',
                name: 'First',
                metadata: {
                    created: new Date('2023-01-01'),
                    tags: ['tag3'],
                },
                nestedObject: {
                    deep: {
                        value: 24,
                    },
                },
            });

            expect(result[1]).toEqual({
                urn: 'urn:li:dataset:2',
                name: 'Second',
                metadata: {
                    created: new Date('2023-01-02'),
                    tags: ['tag1', 'tag2'],
                },
                nestedObject: {
                    deep: {
                        value: 42,
                    },
                },
            });
        });
    });
});

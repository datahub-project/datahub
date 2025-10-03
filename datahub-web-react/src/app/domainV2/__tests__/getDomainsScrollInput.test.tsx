import { DOMAIN_COUNT, getDomainsScrollInput } from '@app/domainV2/useScrollDomains';
import { ENTITY_NAME_FIELD } from '@app/searchV2/context/constants';

import { EntityType, FilterOperator, SortOrder } from '@types';

describe('getDomainsScrollInput', () => {
    describe('Root domains (parentDomain is null)', () => {
        it('should create correct input for root domains with no scrollId', () => {
            const result = getDomainsScrollInput(null, null);

            expect(result).toEqual({
                input: {
                    scrollId: null,
                    query: '*',
                    types: [EntityType.Domain],
                    orFilters: [
                        {
                            and: [
                                {
                                    field: 'parentDomain',
                                    condition: FilterOperator.Exists,
                                    negated: true,
                                },
                            ],
                        },
                    ],
                    count: DOMAIN_COUNT,
                    sortInput: {
                        sortCriteria: [
                            {
                                field: ENTITY_NAME_FIELD,
                                sortOrder: SortOrder.Ascending,
                            },
                        ],
                    },
                    searchFlags: { skipCache: true },
                },
            });
        });

        it('should create correct input for root domains with scrollId', () => {
            const scrollId = 'test-scroll-id-123';
            const result = getDomainsScrollInput(null, scrollId);

            expect(result).toEqual({
                input: {
                    scrollId,
                    query: '*',
                    types: [EntityType.Domain],
                    orFilters: [
                        {
                            and: [
                                {
                                    field: 'parentDomain',
                                    condition: FilterOperator.Exists,
                                    negated: true,
                                },
                            ],
                        },
                    ],
                    count: DOMAIN_COUNT,
                    sortInput: {
                        sortCriteria: [
                            {
                                field: ENTITY_NAME_FIELD,
                                sortOrder: SortOrder.Ascending,
                            },
                        ],
                    },
                    searchFlags: { skipCache: true },
                },
            });
        });
    });

    describe('Child domains (parentDomain is provided)', () => {
        const parentDomainUrn = 'urn:li:domain:parent';

        it('should create correct input for child domains with no scrollId', () => {
            const result = getDomainsScrollInput(parentDomainUrn, null);

            expect(result).toEqual({
                input: {
                    scrollId: null,
                    query: '*',
                    types: [EntityType.Domain],
                    orFilters: [
                        {
                            and: [
                                {
                                    field: 'parentDomain',
                                    values: [parentDomainUrn],
                                },
                            ],
                        },
                    ],
                    count: DOMAIN_COUNT,
                    sortInput: {
                        sortCriteria: [
                            {
                                field: ENTITY_NAME_FIELD,
                                sortOrder: SortOrder.Ascending,
                            },
                        ],
                    },
                    searchFlags: { skipCache: true },
                },
            });
        });

        it('should create correct input for child domains with scrollId', () => {
            const scrollId = 'child-scroll-id-456';
            const result = getDomainsScrollInput(parentDomainUrn, scrollId);

            expect(result).toEqual({
                input: {
                    scrollId,
                    query: '*',
                    types: [EntityType.Domain],
                    orFilters: [
                        {
                            and: [
                                {
                                    field: 'parentDomain',
                                    values: [parentDomainUrn],
                                },
                            ],
                        },
                    ],
                    count: DOMAIN_COUNT,
                    sortInput: {
                        sortCriteria: [
                            {
                                field: ENTITY_NAME_FIELD,
                                sortOrder: SortOrder.Ascending,
                            },
                        ],
                    },
                    searchFlags: { skipCache: true },
                },
            });
        });
    });

    describe('Edge cases', () => {
        it('should handle empty string parentDomain as no parent domain', () => {
            const result = getDomainsScrollInput('', null);

            expect(result.input.orFilters).toEqual([
                {
                    and: [
                        {
                            field: 'parentDomain',
                            condition: FilterOperator.Exists,
                            negated: true,
                        },
                    ],
                },
            ]);
        });

        it('should handle empty string scrollId', () => {
            const result = getDomainsScrollInput(null, '');

            expect(result.input.scrollId).toBe('');
        });

        it('should handle undefined parentDomain same as null', () => {
            const resultNull = getDomainsScrollInput(null, null);
            const resultUndefined = getDomainsScrollInput(undefined as any, null);

            expect(resultNull).toEqual(resultUndefined);
        });
    });

    describe('Configuration consistency', () => {
        it('should always use the same query string', () => {
            const result1 = getDomainsScrollInput(null, null);
            const result2 = getDomainsScrollInput('urn:li:domain:parent', 'scroll-id');

            expect(result1.input.query).toBe('*');
            expect(result2.input.query).toBe('*');
        });

        it('should always include Domain entity type', () => {
            const result1 = getDomainsScrollInput(null, null);
            const result2 = getDomainsScrollInput('urn:li:domain:parent', 'scroll-id');

            expect(result1.input.types).toEqual([EntityType.Domain]);
            expect(result2.input.types).toEqual([EntityType.Domain]);
        });

        it('should always use the same count', () => {
            const result1 = getDomainsScrollInput(null, null);
            const result2 = getDomainsScrollInput('urn:li:domain:parent', 'scroll-id');

            expect(result1.input.count).toBe(DOMAIN_COUNT);
            expect(result2.input.count).toBe(DOMAIN_COUNT);
            expect(result1.input.count).toBe(25); // Verify the constant value
        });

        it('should always use ascending sort by name', () => {
            const result1 = getDomainsScrollInput(null, null);
            const result2 = getDomainsScrollInput('urn:li:domain:parent', 'scroll-id');

            const expectedSortInput = {
                sortCriteria: [
                    {
                        field: ENTITY_NAME_FIELD,
                        sortOrder: SortOrder.Ascending,
                    },
                ],
            };

            expect(result1.input.sortInput).toEqual(expectedSortInput);
            expect(result2.input.sortInput).toEqual(expectedSortInput);
        });

        it('should always skip cache', () => {
            const result1 = getDomainsScrollInput(null, null);
            const result2 = getDomainsScrollInput('urn:li:domain:parent', 'scroll-id');

            expect(result1.input.searchFlags).toEqual({ skipCache: true });
            expect(result2.input.searchFlags).toEqual({ skipCache: true });
        });
    });
});

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

    describe('Owner filter', () => {
        it('omits the owners clause when selectedOwnerUrns is undefined / null / empty', () => {
            // The "no filter" cases should produce the same orFilters shape as
            // calling without the third argument at all.
            const baseline = getDomainsScrollInput(null, null).input.orFilters;

            expect(getDomainsScrollInput(null, null, undefined).input.orFilters).toEqual(baseline);
            expect(getDomainsScrollInput(null, null, null).input.orFilters).toEqual(baseline);
            expect(getDomainsScrollInput(null, null, []).input.orFilters).toEqual(baseline);
        });

        it('ANDs the owners clause with the root-domain scope', () => {
            const result = getDomainsScrollInput(null, null, ['urn:li:corpuser:jane', 'urn:li:corpuser:john']);

            // Single AND clause carrying BOTH the parent scope and the owner
            // selection — server returns root domains that match both.
            expect(result.input.orFilters).toEqual([
                {
                    and: [
                        { field: 'parentDomain', condition: FilterOperator.Exists, negated: true },
                        { field: 'owners', values: ['urn:li:corpuser:jane', 'urn:li:corpuser:john'] },
                    ],
                },
            ]);
        });

        it('ANDs the owners clause with the child-domain scope', () => {
            const result = getDomainsScrollInput('urn:li:domain:parent', null, ['urn:li:corpuser:jane']);

            expect(result.input.orFilters).toEqual([
                {
                    and: [
                        { field: 'parentDomain', values: ['urn:li:domain:parent'] },
                        { field: 'owners', values: ['urn:li:corpuser:jane'] },
                    ],
                },
            ]);
        });
    });

    describe('ignoreParentScope (flat-list mode)', () => {
        it('drops the parentDomain clause entirely so the query spans every depth', () => {
            // This is the bug-fix John flagged: the sidebar's flat-list mode
            // (active when an owner filter is selected) must NOT pin the
            // query to root domains, or matching subdomains would silently
            // disappear from the results.
            const result = getDomainsScrollInput(null, null, ['urn:li:corpuser:jane'], true);

            expect(result.input.orFilters).toEqual([{ and: [{ field: 'owners', values: ['urn:li:corpuser:jane'] }] }]);
        });

        it('drops the parentDomain clause even when a parentDomain argument is supplied', () => {
            // `ignoreParentScope` wins over the parentDomain argument — the
            // caller is explicitly opting into a global query.
            const result = getDomainsScrollInput('urn:li:domain:parent', null, ['urn:li:corpuser:jane'], true);

            expect(result.input.orFilters).toEqual([{ and: [{ field: 'owners', values: ['urn:li:corpuser:jane'] }] }]);
        });

        it('produces an empty AND clause when no filters apply (server treats this as "no filter")', () => {
            const result = getDomainsScrollInput(null, null, undefined, true);

            expect(result.input.orFilters).toEqual([{ and: [] }]);
        });
    });
});

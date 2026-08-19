import { DOMAIN_SIDEBAR_SORT } from '@app/domainV2/nestedDomains/domainSidebarFilters/domainSidebarSort';
import { DOMAIN_COUNT, getDomainsScrollInput } from '@app/domainV2/useScrollDomains';
import { ENTITY_NAME_FIELD } from '@app/searchV2/context/constants';
import { CREATED_TIME_FIELD_NAME } from '@app/searchV2/utils/constants';

import { EntityType, FilterOperator, SortOrder } from '@types';

describe('getDomainsScrollInput', () => {
    describe('Root domains (parentDomain is null)', () => {
        it('should create correct input for root domains with no scrollId', () => {
            const result = getDomainsScrollInput({ parentDomain: null, scrollId: null });

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
            const result = getDomainsScrollInput({ parentDomain: null, scrollId });

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
            const result = getDomainsScrollInput({ parentDomain: parentDomainUrn, scrollId: null });

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
            const result = getDomainsScrollInput({ parentDomain: parentDomainUrn, scrollId });

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
            const result = getDomainsScrollInput({ parentDomain: '', scrollId: null });

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
            const result = getDomainsScrollInput({ parentDomain: null, scrollId: '' });

            expect(result.input.scrollId).toBe('');
        });
    });

    describe('Configuration consistency', () => {
        it('should always use the same query string', () => {
            const result1 = getDomainsScrollInput({ parentDomain: null, scrollId: null });
            const result2 = getDomainsScrollInput({
                parentDomain: 'urn:li:domain:parent',
                scrollId: 'scroll-id',
            });

            expect(result1.input.query).toBe('*');
            expect(result2.input.query).toBe('*');
        });

        it('should always include Domain entity type', () => {
            const result1 = getDomainsScrollInput({ parentDomain: null, scrollId: null });
            const result2 = getDomainsScrollInput({
                parentDomain: 'urn:li:domain:parent',
                scrollId: 'scroll-id',
            });

            expect(result1.input.types).toEqual([EntityType.Domain]);
            expect(result2.input.types).toEqual([EntityType.Domain]);
        });

        it('should always use the same count', () => {
            const result1 = getDomainsScrollInput({ parentDomain: null, scrollId: null });
            const result2 = getDomainsScrollInput({
                parentDomain: 'urn:li:domain:parent',
                scrollId: 'scroll-id',
            });

            expect(result1.input.count).toBe(DOMAIN_COUNT);
            expect(result2.input.count).toBe(DOMAIN_COUNT);
            expect(result1.input.count).toBe(25);
        });

        it('should default to ascending sort by name', () => {
            const result1 = getDomainsScrollInput({ parentDomain: null, scrollId: null });
            const result2 = getDomainsScrollInput({
                parentDomain: 'urn:li:domain:parent',
                scrollId: 'scroll-id',
            });

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
            const result1 = getDomainsScrollInput({ parentDomain: null, scrollId: null });
            const result2 = getDomainsScrollInput({
                parentDomain: 'urn:li:domain:parent',
                scrollId: 'scroll-id',
            });

            expect(result1.input.searchFlags).toEqual({ skipCache: true });
            expect(result2.input.searchFlags).toEqual({ skipCache: true });
        });
    });

    describe('Sort options', () => {
        it('applies name descending when requested', () => {
            const result = getDomainsScrollInput({
                parentDomain: null,
                scrollId: null,
                sort: DOMAIN_SIDEBAR_SORT.NAME_DESC,
            });

            expect(result.input.sortInput).toEqual({
                sortCriteria: [{ field: ENTITY_NAME_FIELD, sortOrder: SortOrder.Descending }],
            });
        });

        it('applies created descending when requested', () => {
            const result = getDomainsScrollInput({
                parentDomain: null,
                scrollId: null,
                sort: DOMAIN_SIDEBAR_SORT.CREATED_DESC,
            });

            expect(result.input.sortInput).toEqual({
                sortCriteria: [{ field: CREATED_TIME_FIELD_NAME, sortOrder: SortOrder.Descending }],
            });
        });
    });

    describe('Owner filter', () => {
        it('omits the owners clause when selectedOwnerUrns is undefined / null / empty', () => {
            const baseline = getDomainsScrollInput({ parentDomain: null, scrollId: null }).input.orFilters;

            expect(
                getDomainsScrollInput({ parentDomain: null, scrollId: null, selectedOwnerUrns: undefined }).input
                    .orFilters,
            ).toEqual(baseline);
            expect(
                getDomainsScrollInput({ parentDomain: null, scrollId: null, selectedOwnerUrns: null }).input.orFilters,
            ).toEqual(baseline);
            expect(
                getDomainsScrollInput({ parentDomain: null, scrollId: null, selectedOwnerUrns: [] }).input.orFilters,
            ).toEqual(baseline);
        });

        it('ANDs the owners clause with the root-domain scope', () => {
            const result = getDomainsScrollInput({
                parentDomain: null,
                scrollId: null,
                selectedOwnerUrns: ['urn:li:corpuser:jane', 'urn:li:corpuser:john'],
            });

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
            const result = getDomainsScrollInput({
                parentDomain: 'urn:li:domain:parent',
                scrollId: null,
                selectedOwnerUrns: ['urn:li:corpuser:jane'],
            });

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
            const result = getDomainsScrollInput({
                parentDomain: null,
                scrollId: null,
                selectedOwnerUrns: ['urn:li:corpuser:jane'],
                ignoreParentScope: true,
            });

            expect(result.input.orFilters).toEqual([{ and: [{ field: 'owners', values: ['urn:li:corpuser:jane'] }] }]);
        });

        it('drops the parentDomain clause even when a parentDomain argument is supplied', () => {
            const result = getDomainsScrollInput({
                parentDomain: 'urn:li:domain:parent',
                scrollId: null,
                selectedOwnerUrns: ['urn:li:corpuser:jane'],
                ignoreParentScope: true,
            });

            expect(result.input.orFilters).toEqual([{ and: [{ field: 'owners', values: ['urn:li:corpuser:jane'] }] }]);
        });

        it('produces an empty AND clause when no filters apply (server treats this as "no filter")', () => {
            const result = getDomainsScrollInput({
                parentDomain: null,
                scrollId: null,
                ignoreParentScope: true,
            });

            expect(result.input.orFilters).toEqual([{ and: [] }]);
        });
    });
});

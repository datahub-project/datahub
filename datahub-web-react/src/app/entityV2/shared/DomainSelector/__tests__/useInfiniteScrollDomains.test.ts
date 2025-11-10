import { describe, expect, it } from 'vitest';

import {
    DOMAIN_SELECTOR_COUNT,
    getDomainSelectorScrollInput,
} from '@app/entityV2/shared/DomainSelector/useInfiniteScrollDomains';
import { EntityType, FilterOperator, SortOrder } from '@src/types.generated';

/**
 * Unit tests for useInfiniteScrollDomains utility functions
 * Testing the helper function getDomainSelectorScrollInput which generates
 * the GraphQL query input for fetching domains with infinite scroll support
 */
describe('getDomainSelectorScrollInput', () => {
    it('should create input for root domains (no parent)', () => {
        const input = getDomainSelectorScrollInput(null, null);

        expect(input).toEqual({
            input: {
                scrollId: null,
                query: '*',
                types: [EntityType.Domain],
                orFilters: [{ and: [{ field: 'parentDomain', condition: FilterOperator.Exists, negated: true }] }],
                count: DOMAIN_SELECTOR_COUNT,
                sortInput: {
                    sortCriteria: [{ field: '_entityName', sortOrder: SortOrder.Ascending }],
                },
                searchFlags: { skipCache: true },
            },
        });
    });

    it('should create input with parent domain filter', () => {
        const parentUrn = 'urn:li:domain:engineering';
        const input = getDomainSelectorScrollInput(parentUrn, null);

        expect(input).toEqual({
            input: {
                scrollId: null,
                query: '*',
                types: [EntityType.Domain],
                orFilters: [{ and: [{ field: 'parentDomain', values: [parentUrn] }] }],
                count: DOMAIN_SELECTOR_COUNT,
                sortInput: {
                    sortCriteria: [{ field: '_entityName', sortOrder: SortOrder.Ascending }],
                },
                searchFlags: { skipCache: true },
            },
        });
    });

    it('should include scrollId when provided', () => {
        const scrollId = 'scroll-123';
        const input = getDomainSelectorScrollInput(null, scrollId);

        expect(input.input.scrollId).toBe(scrollId);
    });

    it('should use correct count value', () => {
        const input = getDomainSelectorScrollInput(null, null);

        expect(input.input.count).toBe(DOMAIN_SELECTOR_COUNT);
        expect(DOMAIN_SELECTOR_COUNT).toBe(2);
    });

    it('should set correct query and types', () => {
        const input = getDomainSelectorScrollInput(null, null);

        expect(input.input.query).toBe('*');
        expect(input.input.types).toEqual([EntityType.Domain]);
    });

    it('should enable cache skipping', () => {
        const input = getDomainSelectorScrollInput(null, null);

        expect(input.input.searchFlags).toEqual({ skipCache: true });
    });

    it('should sort by entity name in ascending order', () => {
        const input = getDomainSelectorScrollInput(null, null);

        expect(input.input.sortInput).toEqual({
            sortCriteria: [{ field: '_entityName', sortOrder: SortOrder.Ascending }],
        });
    });

    it('should handle multiple parent domains correctly', () => {
        const parentUrn1 = 'urn:li:domain:engineering';
        const parentUrn2 = 'urn:li:domain:marketing';

        const input1 = getDomainSelectorScrollInput(parentUrn1, null);
        const input2 = getDomainSelectorScrollInput(parentUrn2, null);

        expect(input1.input.orFilters).toEqual([{ and: [{ field: 'parentDomain', values: [parentUrn1] }] }]);
        expect(input2.input.orFilters).toEqual([{ and: [{ field: 'parentDomain', values: [parentUrn2] }] }]);
    });

    it('should handle both parent domain and scrollId together', () => {
        const parentUrn = 'urn:li:domain:engineering';
        const scrollId = 'scroll-456';

        const input = getDomainSelectorScrollInput(parentUrn, scrollId);

        expect(input.input.scrollId).toBe(scrollId);
        expect(input.input.orFilters).toEqual([{ and: [{ field: 'parentDomain', values: [parentUrn] }] }]);
    });

    it('should create different filters for root vs child domains', () => {
        const rootInput = getDomainSelectorScrollInput(null, null);
        const childInput = getDomainSelectorScrollInput('urn:li:domain:parent', null);

        // Root domains: filter where parentDomain does NOT exist
        expect(rootInput.input.orFilters).toEqual([
            { and: [{ field: 'parentDomain', condition: FilterOperator.Exists, negated: true }] },
        ]);

        // Child domains: filter where parentDomain equals specific value
        expect(childInput.input.orFilters).toEqual([
            { and: [{ field: 'parentDomain', values: ['urn:li:domain:parent'] }] },
        ]);
    });

    it('should maintain immutability - separate calls return separate objects', () => {
        const input1 = getDomainSelectorScrollInput(null, null);
        const input2 = getDomainSelectorScrollInput(null, null);

        expect(input1).not.toBe(input2);
        expect(input1.input).not.toBe(input2.input);
        expect(input1).toEqual(input2);
    });
});

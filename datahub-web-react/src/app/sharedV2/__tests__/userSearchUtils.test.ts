import { describe, expect, it } from 'vitest';

import { ENTITY_FILTER_NAME } from '@app/searchV2/utils/constants';
import { addUserFiltersToMultiEntitySearchInput, getUserFilters } from '@app/sharedV2/userSearchUtils';

import { EntityType, FilterOperator } from '@types';

const USER_CLAUSES = [
    { and: [{ field: 'active', values: ['true'] }] },
    { and: [{ field: 'displayName', condition: FilterOperator.Exists }] },
];

describe('getUserFilters', () => {
    it('only filters users when users are the only type searched', () => {
        expect(getUserFilters([EntityType.CorpUser])).toEqual(USER_CLAUSES);
    });

    it('passes the other searched types through by entity type', () => {
        expect(getUserFilters([EntityType.CorpUser, EntityType.Role, EntityType.Dashboard])).toEqual([
            ...USER_CLAUSES,
            { and: [{ field: ENTITY_FILTER_NAME, values: [EntityType.Role, EntityType.Dashboard] }] },
        ]);
    });
});

describe('addUserFiltersToMultiEntitySearchInput', () => {
    it('leaves searches without users unfiltered', () => {
        const input = { query: 'x', types: [EntityType.Dataset] };
        expect(addUserFiltersToMultiEntitySearchInput(input, [EntityType.Dataset])).toBe(input);
    });
});

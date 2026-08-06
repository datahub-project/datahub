import { describe, expect, it } from 'vitest';

import {
    DOMAIN_SIDEBAR_SORT,
    domainSidebarSortToCriterion,
} from '@app/domainV2/nestedDomains/domainSidebarFilters/domainSidebarSort';
import { ENTITY_NAME_FIELD } from '@app/searchV2/context/constants';
import { CREATED_TIME_FIELD_NAME } from '@app/searchV2/utils/constants';

import { SortOrder } from '@types';

describe('domainSidebarSortToCriterion', () => {
    it('maps name and created selections to search sortCriterion', () => {
        expect(domainSidebarSortToCriterion(DOMAIN_SIDEBAR_SORT.NAME_ASC)).toEqual({
            field: ENTITY_NAME_FIELD,
            sortOrder: SortOrder.Ascending,
        });
        expect(domainSidebarSortToCriterion(DOMAIN_SIDEBAR_SORT.NAME_DESC)).toEqual({
            field: ENTITY_NAME_FIELD,
            sortOrder: SortOrder.Descending,
        });
        expect(domainSidebarSortToCriterion(DOMAIN_SIDEBAR_SORT.CREATED_DESC)).toEqual({
            field: CREATED_TIME_FIELD_NAME,
            sortOrder: SortOrder.Descending,
        });
    });
});

import { describe, expect, it } from 'vitest';

import {
    DEFAULT_GLOSSARY_SIDEBAR_SORT,
    GLOSSARY_SIDEBAR_SORT,
    glossarySidebarSortToNameCriterion,
} from '@app/glossaryV2/glossarySidebarFilters/glossarySidebarSort';
import { ENTITY_NAME_FIELD } from '@app/searchV2/context/constants';

import { SortOrder } from '@types';

describe('glossarySidebarSortToNameCriterion', () => {
    it('maps name selections to search sortCriterion', () => {
        expect(glossarySidebarSortToNameCriterion(GLOSSARY_SIDEBAR_SORT.NAME_ASC)).toEqual({
            field: ENTITY_NAME_FIELD,
            sortOrder: SortOrder.Ascending,
        });
        expect(glossarySidebarSortToNameCriterion(GLOSSARY_SIDEBAR_SORT.NAME_DESC)).toEqual({
            field: ENTITY_NAME_FIELD,
            sortOrder: SortOrder.Descending,
        });
    });

    it('defaults to name ascending', () => {
        expect(DEFAULT_GLOSSARY_SIDEBAR_SORT).toBe(GLOSSARY_SIDEBAR_SORT.NAME_ASC);
    });
});

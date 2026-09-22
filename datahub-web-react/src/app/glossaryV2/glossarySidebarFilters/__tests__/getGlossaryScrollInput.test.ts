import { describe, expect, it } from 'vitest';

import { getGlossaryScrollInput } from '@app/glossaryV2/glossarySidebarFilters/getGlossaryScrollInput';
import { GLOSSARY_SIDEBAR_SORT } from '@app/glossaryV2/glossarySidebarFilters/glossarySidebarSort';
import { ENTITY_INDEX_FILTER_NAME } from '@app/search/utils/constants';
import { ENTITY_NAME_FIELD } from '@app/searchV2/context/constants';
import { DOMAINS_FILTER_NAME, OWNERS_FILTER_NAME, TAGS_FILTER_NAME } from '@app/searchV2/utils/constants';

import { EntityType, FilterOperator, SortOrder } from '@types';

describe('getGlossaryScrollInput', () => {
    it('scopes to roots when parentNode is null', () => {
        const result = getGlossaryScrollInput({ parentNode: null });
        expect(result.input.orFilters).toEqual([
            {
                and: [{ field: 'parentNode', condition: FilterOperator.Exists, negated: true }],
            },
        ]);
        expect(result.input.types).toEqual([EntityType.GlossaryNode, EntityType.GlossaryTerm]);
    });

    it('scopes to children of a parent node', () => {
        const result = getGlossaryScrollInput({ parentNode: 'urn:li:glossaryNode:parent' });
        expect(result.input.orFilters).toEqual([
            { and: [{ field: 'parentNode', values: ['urn:li:glossaryNode:parent'] }] },
        ]);
    });

    it('ignores parent scope in flat filter mode', () => {
        const result = getGlossaryScrollInput({
            parentNode: null,
            ignoreParentScope: true,
            selectedOwnerUrns: ['urn:li:corpuser:jane'],
        });
        expect(result.input.orFilters).toEqual([
            { and: [{ field: OWNERS_FILTER_NAME, values: ['urn:li:corpuser:jane'] }] },
        ]);
    });

    it('ANDs owners, tags, and domains filters', () => {
        const result = getGlossaryScrollInput({
            parentNode: null,
            ignoreParentScope: true,
            selectedOwnerUrns: ['urn:li:corpuser:jane'],
            selectedTagUrns: ['urn:li:tag:pii'],
            selectedDomainUrns: ['urn:li:domain:marketing'],
        });
        expect(result.input.orFilters).toEqual([
            {
                and: [
                    { field: OWNERS_FILTER_NAME, values: ['urn:li:corpuser:jane'] },
                    { field: TAGS_FILTER_NAME, values: ['urn:li:tag:pii'] },
                    { field: DOMAINS_FILTER_NAME, values: ['urn:li:domain:marketing'] },
                ],
            },
        ]);
    });

    it('sorts by type then name by default', () => {
        const result = getGlossaryScrollInput({ parentNode: null });
        expect(result.input.sortInput).toEqual({
            sortCriteria: [
                { field: ENTITY_INDEX_FILTER_NAME, sortOrder: SortOrder.Ascending },
                { field: ENTITY_NAME_FIELD, sortOrder: SortOrder.Ascending },
            ],
        });
    });

    it('applies name descending and can skip type-before-name', () => {
        const result = getGlossaryScrollInput({
            parentNode: null,
            ignoreParentScope: true,
            sort: GLOSSARY_SIDEBAR_SORT.NAME_DESC,
            sortTypeBeforeName: false,
        });
        expect(result.input.sortInput).toEqual({
            sortCriteria: [{ field: ENTITY_NAME_FIELD, sortOrder: SortOrder.Descending }],
        });
    });
});

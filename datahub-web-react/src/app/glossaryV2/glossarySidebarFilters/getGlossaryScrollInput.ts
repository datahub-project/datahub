import {
    DEFAULT_GLOSSARY_SIDEBAR_SORT,
    GlossarySidebarSortValue,
    glossarySidebarSortToNameCriterion,
} from '@app/glossaryV2/glossarySidebarFilters/glossarySidebarSort';
import { DEFAULT_GLOSSARY_CHILDREN_COUNT } from '@app/glossaryV2/utils';
import { ENTITY_INDEX_FILTER_NAME } from '@app/search/utils/constants';
import { DOMAINS_FILTER_NAME, OWNERS_FILTER_NAME, TAGS_FILTER_NAME } from '@app/searchV2/utils/constants';

import { ScrollAcrossEntitiesQueryVariables } from '@graphql/search.generated';
import { EntityType, FacetFilterInput, FilterOperator, SortOrder } from '@types';

export const GLOSSARY_SCROLL_COUNT = DEFAULT_GLOSSARY_CHILDREN_COUNT;

export type GetGlossaryScrollInputArgs = {
    /** Parent node URN for children; null = roots (`parentNode` NOT EXISTS). Ignored when `ignoreParentScope`. */
    parentNode: string | null;
    scrollId?: string | null;
    sort?: GlossarySidebarSortValue;
    selectedOwnerUrns?: ReadonlyArray<string> | null;
    selectedTagUrns?: ReadonlyArray<string> | null;
    selectedDomainUrns?: ReadonlyArray<string> | null;
    /**
     * Drop the parentNode clause so results span every glossary entity at every
     * depth — used by flat filter mode.
     */
    ignoreParentScope?: boolean;
    count?: number;
    /**
     * When true (default for tree children), keep `_entityType` ASC before name
     * so term groups appear above terms. Flat filtered lists skip this so a
     * single name sort applies across both types.
     */
    sortTypeBeforeName?: boolean;
};

export function getGlossaryScrollInput({
    parentNode,
    scrollId = null,
    sort = DEFAULT_GLOSSARY_SIDEBAR_SORT,
    selectedOwnerUrns,
    selectedTagUrns,
    selectedDomainUrns,
    ignoreParentScope = false,
    count = GLOSSARY_SCROLL_COUNT,
    sortTypeBeforeName = true,
}: GetGlossaryScrollInputArgs): ScrollAcrossEntitiesQueryVariables {
    const filters: FacetFilterInput[] = [];

    if (!ignoreParentScope) {
        const parentFilter: FacetFilterInput = parentNode
            ? { field: 'parentNode', values: [parentNode] }
            : { field: 'parentNode', condition: FilterOperator.Exists, negated: true };
        filters.push(parentFilter);
    }

    if (selectedOwnerUrns && selectedOwnerUrns.length > 0) {
        filters.push({ field: OWNERS_FILTER_NAME, values: [...selectedOwnerUrns] });
    }

    if (selectedTagUrns && selectedTagUrns.length > 0) {
        filters.push({ field: TAGS_FILTER_NAME, values: [...selectedTagUrns] });
    }

    if (selectedDomainUrns && selectedDomainUrns.length > 0) {
        filters.push({ field: DOMAINS_FILTER_NAME, values: [...selectedDomainUrns] });
    }

    const nameCriterion = glossarySidebarSortToNameCriterion(sort);
    const sortCriteria = sortTypeBeforeName
        ? [{ field: ENTITY_INDEX_FILTER_NAME, sortOrder: SortOrder.Ascending }, nameCriterion]
        : [nameCriterion];

    return {
        input: {
            scrollId,
            query: '*',
            types: [EntityType.GlossaryNode, EntityType.GlossaryTerm],
            orFilters: [{ and: filters }],
            count,
            sortInput: { sortCriteria },
            searchFlags: { skipCache: true },
        },
    };
}

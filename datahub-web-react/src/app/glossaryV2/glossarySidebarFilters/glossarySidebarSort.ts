import { ENTITY_NAME_FIELD } from '@app/searchV2/context/constants';

import { SortCriterion, SortOrder } from '@types';

/** Glossary sidebar sort option values (also used as menu keys). */
export const GLOSSARY_SIDEBAR_SORT = {
    NAME_ASC: 'name_asc',
    NAME_DESC: 'name_desc',
} as const;

export type GlossarySidebarSortValue = (typeof GLOSSARY_SIDEBAR_SORT)[keyof typeof GLOSSARY_SIDEBAR_SORT];

/** Preserve historical tree order (name A–Z) as the default. */
export const DEFAULT_GLOSSARY_SIDEBAR_SORT: GlossarySidebarSortValue = GLOSSARY_SIDEBAR_SORT.NAME_ASC;

/**
 * Glossary entities are sorted by `_entityName`. Root / child tree lists also keep
 * `_entityType` ASC first so term groups stay above terms.
 * Pass via scrollAcrossEntities sortInput — never reorder client-side in the sidebar.
 */
export function glossarySidebarSortToNameCriterion(sort: GlossarySidebarSortValue): SortCriterion {
    switch (sort) {
        case GLOSSARY_SIDEBAR_SORT.NAME_ASC:
            return { field: ENTITY_NAME_FIELD, sortOrder: SortOrder.Ascending };
        case GLOSSARY_SIDEBAR_SORT.NAME_DESC:
            return { field: ENTITY_NAME_FIELD, sortOrder: SortOrder.Descending };
        default: {
            const exhaustiveCheck: never = sort;
            throw new Error(`Unhandled glossary sidebar sort: ${exhaustiveCheck}`);
        }
    }
}

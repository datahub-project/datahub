import { ENTITY_FILTER_NAME } from '@app/searchV2/utils/constants';

import {
    AndFilterInput,
    AutoCompleteInput,
    AutoCompleteMultipleInput,
    EntityType,
    FilterOperator,
    SearchAcrossEntitiesInput,
} from '@types';

/**
 * Returns orFilters that implement "active OR displayName" logic.
 *
 * These orFilters are applied identically across every requested entity type (the search/autocomplete
 * resolvers do not scope filters per-type), so any non-CorpUser type mixed into the same request needs
 * its own passthrough clause here or it gets silently filtered out for lacking `active`/`displayName`.
 *
 * @param entityTypes - Array of entity types being searched, used to add passthrough clauses for
 *     non-CorpUser types that would otherwise be excluded by the active/displayName checks above
 * @returns Array of AndFilterInput for server-side user filtering
 */
export const getUserFilters = (entityTypes: EntityType[] = []): AndFilterInput[] => {
    const filters: AndFilterInput[] = [
        {
            // Include users with active status = true
            and: [{ field: 'active', values: ['true'] }],
        },
        {
            // Include users with displayName populated (regardless of active status)
            and: [{ field: 'displayName', condition: FilterOperator.Exists }],
        },
    ];
    const nonUserTypes = entityTypes.filter((type) => type !== EntityType.CorpUser);
    if (nonUserTypes.length) {
        // The active/displayName checks are meant for users only. Other types in the same mixed-type request
        // (roles, dashboards, glossary terms...) mostly have neither field, so pass them through by type
        // rather than silently dropping them.
        filters.push({ and: [{ field: ENTITY_FILTER_NAME, values: nonUserTypes }] });
    }
    return filters;
};

/**
 * Adds filtering on active=true or displayName to SearchAcrossEntitiesInput when searching for CorpUser entities.
 *
 * @param input - The search input object to modify
 * @param entityTypes - Array of entity types being searched
 * @returns The modified input object with user filters applied if CorpUser is included
 */
export const addUserFiltersToMultiEntitySearchInput = (
    input: SearchAcrossEntitiesInput,
    entityTypes: EntityType[],
): SearchAcrossEntitiesInput => {
    if (entityTypes.includes(EntityType.CorpUser)) {
        return {
            ...input,
            orFilters: getUserFilters(entityTypes),
        };
    }
    return input;
};

/**
 * Adds filtering on active=true or displayName to AutoCompleteInput when the entity type is CorpUser.
 *
 * @param input - The autocomplete input object to modify
 * @param entityType - The entity type being searched
 * @returns The modified input object with user filters applied if needed
 */
export const addUserFiltersToAutoCompleteInput = (
    input: AutoCompleteInput,
    entityType: EntityType,
): AutoCompleteInput => {
    if (entityType === EntityType.CorpUser) {
        return {
            ...input,
            orFilters: getUserFilters([entityType]),
        };
    }
    return input;
};

export const addUserFiltersToAutoCompleteMultipleInput = (
    input: AutoCompleteMultipleInput,
    entityTypes: EntityType[],
): AutoCompleteMultipleInput => {
    if (entityTypes.includes(EntityType.CorpUser)) {
        return {
            ...input,
            orFilters: getUserFilters(entityTypes),
        };
    }
    return input;
};

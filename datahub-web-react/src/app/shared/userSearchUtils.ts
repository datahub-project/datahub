import {
    AndFilterInput,
    AutoCompleteInput,
    AutoCompleteMultipleInput,
    EntityType,
    FilterOperator,
    SearchAcrossEntitiesInput,
    SearchInput,
} from '@types';

/**
 * Returns orFilters that implement "active OR displayName" logic.
 *
 * @returns Array of AndFilterInput for server-side user filtering
 */
const getUserFilters = (): AndFilterInput[] => [
    {
        // Include users with active status = true
        and: [{ field: 'active', values: ['true'] }],
    },
    {
        // Include users with displayName populated (regardless of active status)
        and: [{ field: 'displayName', condition: FilterOperator.Exists }],
    },
];

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
            orFilters: getUserFilters(),
        };
    }
    return input;
};

/**
 * Adds filtering on active=true or displayName to SearchInput when the entity type is CorpUser.
 *
 * @param input - The search input object to modify
 * @param entityType - The entity type being searched
 * @returns The modified input object with user filters applied if needed
 */
export const addUserFiltersToSearchInput = (input: SearchInput, entityType: EntityType): SearchInput => {
    if (entityType === EntityType.CorpUser) {
        return {
            ...input,
            orFilters: getUserFilters(),
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
            orFilters: getUserFilters(),
        };
    }
    return input;
};

/**
 * Adds filtering on active=true or displayName to AutoCompleteMultipleInput when CorpUser is included in types.
 *
 * @param input - The autocomplete input object to modify
 * @param entityTypes - Array of entity types being searched
 * @returns The modified input object with user filters applied if CorpUser is included
 */
const addUserFiltersToAutoCompleteMultipleInput = (
    input: AutoCompleteMultipleInput,
    entityTypes: EntityType[],
): AutoCompleteMultipleInput => {
    if (entityTypes.includes(EntityType.CorpUser)) {
        return {
            ...input,
            orFilters: getUserFilters(),
        };
    }
    return input;
};

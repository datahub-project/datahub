import { Entity } from '@types';

/**
 * Flattens autocomplete suggestions from GraphQL into a simple array of entities
 * @param suggestions Array of suggestion objects from autoCompleteForMultiple query
 * @returns Flattened array of entities
 */
export const flattenAutocompleteSuggestions = (suggestions: any[]): Entity[] => {
    // Handle null, undefined, or non-array input
    if (!suggestions || !Array.isArray(suggestions)) {
        return [];
    }

    const entities: Entity[] = [];

    suggestions.forEach((suggestion) => {
        const { entities: suggestionEntities } = suggestion;
        if (suggestionEntities && suggestionEntities.length > 0) {
            entities.push(...suggestionEntities);
        }
    });

    return entities;
};

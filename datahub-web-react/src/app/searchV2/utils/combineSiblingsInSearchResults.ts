import { CombinedEntity, createSiblingEntityCombiner } from '@app/entity/shared/siblingUtils';

import { Entity, EntityPath, MatchedField } from '@types';

// Restore type hints for safety
type MaybeEntityWithPlatform = Entity & { platform?: { name?: string }; urn?: string };
type MaybeEntityWithSiblings = Entity & { siblingsSearch?: { searchResults?: { entity?: MaybeEntityWithPlatform }[] } };

type UncombinedSeaerchResults = {
    entity: MaybeEntityWithSiblings; // Restore specific type
    matchedFields: Array<MatchedField>;
    paths?: EntityPath[];
};

export type CombinedSearchResult = CombinedEntity &
    Pick<UncombinedSeaerchResults, 'matchedFields'> &
    Pick<UncombinedSeaerchResults, 'paths'>;

export function combineSiblingsInSearchResults(
    showSeparateSiblings: boolean,
    searchResults: Array<UncombinedSeaerchResults> | undefined = [],
): Array<CombinedSearchResult> {
    const combine = createSiblingEntityCombiner();
    const combinedSearchResults: Array<CombinedSearchResult> = [];

    // Create a set of URNs from the original search results for quick lookup
    const originalUrns = new Set(searchResults.map((result) => result.entity.urn));

    searchResults.forEach((searchResult) => {
        let entityToProcess = searchResult.entity;

        // Check if there is exactly one sibling search result and it's from snowflake
        // @ts-ignore
        const siblingsSearch = entityToProcess?.siblingsSearch;
        const siblingEntity = siblingsSearch?.searchResults?.[0]?.entity;

        if (
            siblingsSearch?.searchResults?.length === 1 &&
            siblingEntity && // Ensure sibling entity exists
            'platform' in siblingEntity && // Type guard: Check for platform property
            siblingEntity.platform?.name === 'snowflake' &&
            siblingEntity.urn && originalUrns.has(siblingEntity.urn) // Check if snowflake sibling URN is in original results
        ) {
            // Use the snowflake sibling's URN
            entityToProcess = {
                ...entityToProcess,
                urn: siblingEntity.urn,
            };
        }

        const combinedResult = showSeparateSiblings
            ? { combinedEntity: entityToProcess, skipped: false }
            : combine(entityToProcess);
        if (!combinedResult.skipped) {
            combinedSearchResults.push({
                ...searchResult,
                ...combinedResult.combinedEntity,
            });
        }
    });

    return combinedSearchResults;
}

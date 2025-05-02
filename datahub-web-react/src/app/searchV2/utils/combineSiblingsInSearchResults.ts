import { CombinedEntity, createSiblingEntityCombiner } from '@app/entity/shared/siblingUtils';

import { Entity, EntityPath, MatchedField } from '@types';

type UncombinedSeaerchResults = {
    entity: Entity;
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
            siblingEntity.urn // Ensure the sibling urn exists
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

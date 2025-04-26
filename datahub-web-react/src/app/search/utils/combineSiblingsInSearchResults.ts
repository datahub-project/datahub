import { CombinedEntity, createSiblingEntityCombiner } from '@app/entity/shared/siblingUtils';

import { Entity, MatchedField } from '@types';

type UncombinedSeaerchResults = {
    entity: Entity;
    matchedFields: Array<MatchedField>;
};

export type CombinedSearchResult = CombinedEntity & Pick<UncombinedSeaerchResults, 'matchedFields'>;

export function combineSiblingsInSearchResults(
    showSeparateSiblings: boolean,
    searchResults: Array<UncombinedSeaerchResults> | undefined = [],
): Array<CombinedSearchResult> {
    const combine = createSiblingEntityCombiner();
    const combinedSearchResults: Array<CombinedSearchResult> = [];

    searchResults.forEach((searchResult) => {
        const combinedResult = showSeparateSiblings
            ? { combinedEntity: searchResult.entity, skipped: false }
            : combine(searchResult.entity);
        if (!combinedResult.skipped) {
            combinedSearchResults.push({
                ...searchResult,
                ...combinedResult.combinedEntity,
            });
        }
    });

    return combinedSearchResults;
}

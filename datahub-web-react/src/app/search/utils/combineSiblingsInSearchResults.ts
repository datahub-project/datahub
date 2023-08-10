import { Entity, MatchedField } from '../../../types.generated';
import { CombinedEntity, createSiblingEntityCombiner } from '../../entity/shared/siblingUtils';

type UncombinedSeaerchResults = {
    entity: Entity;
    matchedFields: Array<MatchedField>;
};

export type CombinedSearchResult = CombinedEntity & {
    matchedFields: Array<MatchedField>;
};

export function combineSiblingsInSearchResults(
    searchResults: Array<UncombinedSeaerchResults> | undefined = [],
): Array<CombinedSearchResult> {
    const combine = createSiblingEntityCombiner();
    const combinedResults: Array<CombinedSearchResult> = [];

    searchResults.forEach(({ entity, matchedFields }) => {
        const combinedEntity = combine(entity);
        if (combinedEntity) {
            combinedResults.push({
                entity: combinedEntity.entity,
                matchedEntities: combinedEntity.matchedEntities,
                matchedFields,
            });
        }
    });

    return combinedResults;
}

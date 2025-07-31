import { CombinedEntity, createSiblingEntityCombiner } from '@app/entity/shared/siblingUtils';

import { Entity, EntityPath, MatchedField } from '@types';

type MaybeEntityWithPlatform = Entity & { platform?: { name?: string }; urn?: string };
type MaybeEntityWithSiblings = Entity & { platform?: { name?: string }; urn?: string } & {
    siblingsSearch?: { searchResults?: { entity?: MaybeEntityWithPlatform }[] };
};

type UncombinedSeaerchResults = {
    entity: MaybeEntityWithSiblings;
    matchedFields: Array<MatchedField>;
    paths?: EntityPath[];
};

const DBT_PLATFORM = 'dbt';

export type CombinedSearchResult = CombinedEntity &
    Pick<UncombinedSeaerchResults, 'matchedFields'> &
    Pick<UncombinedSeaerchResults, 'paths'>;

export function combineSiblingsInSearchResults(
    showSeparateSiblings: boolean,
    searchResults: Array<UncombinedSeaerchResults> | undefined = [],
): Array<CombinedSearchResult> {
    const combine = createSiblingEntityCombiner();
    const combinedSearchResults: Array<CombinedSearchResult> = [];

    const originalUrnList = searchResults.map((result) => result.entity.urn);

    // With usage propagation from snowflake to dbt, dbt and snowflake siblings do not rank
    // consistently in search. in cases where they both show up in the same page, we should have
    // the warehouse urn win for consistency.
    searchResults.forEach((searchResult, index) => {
        let entityToProcess = searchResult.entity;

        // If showing separate siblings, skip the complex logic and just add the entity
        if (showSeparateSiblings) {
            combinedSearchResults.push({
                ...searchResult,
                ...entityToProcess,
            });
            return;
        }

        // properly handle cases where non-dbt and dbt siblings are both present in results.
        const siblingUrn = entityToProcess?.siblingsSearch?.searchResults?.[0]?.entity?.urn || '';
        const hasSiblingInList = originalUrnList.includes(siblingUrn);
        const entityPlatform = entityToProcess?.platform?.name;
        const siblingPlatform = entityToProcess?.siblingsSearch?.searchResults?.[0]?.entity?.platform?.name;
        const isDbtAndWarehouseSibling = entityPlatform === DBT_PLATFORM && siblingPlatform !== DBT_PLATFORM;

        const siblingIndex = siblingUrn ? originalUrnList.indexOf(siblingUrn) : -1;
        const isSiblingAheadInTheList = siblingIndex !== -1 && siblingIndex < index;

        if (hasSiblingInList && isDbtAndWarehouseSibling) {
            // Skip if sibling is ahead in the list (already rendered or will be rendered with warehouse URN)
            if (isSiblingAheadInTheList) {
                return;
            }
            
            // If this is a DBT entity and sibling is not ahead, use the warehouse URN
            if (entityPlatform === DBT_PLATFORM) {
                entityToProcess = {
                    ...entityToProcess,
                    urn: siblingUrn,
                };
            }
            // Non-DBT entities render normally (no action needed)
        }

        const combinedResult = combine(entityToProcess);
        if (!combinedResult.skipped) {
            combinedSearchResults.push({
                ...searchResult,
                ...combinedResult.combinedEntity,
            });
        }
    });

    return combinedSearchResults;
}

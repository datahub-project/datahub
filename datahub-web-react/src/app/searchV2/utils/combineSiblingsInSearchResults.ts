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

    // we need to apply this fix up because
    searchResults.forEach((searchResult) => {
        let entityToProcess = searchResult.entity;

        // properly handle cases where non-dbt and dbt siblings are both present in results.
        const siblingUrn = entityToProcess?.siblingsSearch?.searchResults?.[0]?.entity?.urn || 'not-available';
        const hasSiblingInList = originalUrnList.includes(siblingUrn);
        const entityPlatform = entityToProcess?.platform?.name;
        const siblingPlatform = entityToProcess?.siblingsSearch?.searchResults?.[0]?.entity?.platform?.name;
        const isDbtAndWarehouseSibling = entityPlatform === DBT_PLATFORM && siblingPlatform !== DBT_PLATFORM;

        const isSiblingAheadInTheList =
            originalUrnList.indexOf(siblingUrn) < originalUrnList.indexOf(entityToProcess?.urn);

        if (hasSiblingInList && isDbtAndWarehouseSibling && !showSeparateSiblings) {
            if (entityPlatform === DBT_PLATFORM && isSiblingAheadInTheList) {
                // skip this- the warehouse is already rendering above
                return;
            }
            if (entityPlatform === DBT_PLATFORM && !isSiblingAheadInTheList) {
                // Use the warehouse sibling's URN
                // render with warehouse urn and skip dbt result
                entityToProcess = {
                    ...entityToProcess,
                    urn: siblingUrn,
                };
            }
            if (entityPlatform !== DBT_PLATFORM && !isSiblingAheadInTheList) {
                // do nothing - render as normal
            }
            if (entityPlatform !== DBT_PLATFORM && isSiblingAheadInTheList) {
                // hide this because dbt will render above with warehouse urn
                return;
            }
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

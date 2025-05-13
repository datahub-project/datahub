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

    searchResults.forEach((searchResult) => {
        let entityToProcess = searchResult.entity;

        // properly handle cases where snowflake and dbt siblings are both present in results.
        const siblingUrn = entityToProcess?.siblingsSearch?.searchResults?.[0]?.entity?.urn || 'not-available';
        const hasSiblingInList = originalUrnList.includes(siblingUrn);
        const entityPlatform = entityToProcess?.platform?.name;
        const isSiblingAheadInTheList =
            originalUrnList.indexOf(siblingUrn) < originalUrnList.indexOf(entityToProcess?.urn);

        if (hasSiblingInList && !showSeparateSiblings) {
            if (entityPlatform === 'dbt' && isSiblingAheadInTheList) {
                // skip this- the snowflake will render
                return;
            }
            if (entityPlatform === 'dbt' && !isSiblingAheadInTheList) {
                // Use the snowflake sibling's URN
                // render with snowflake urn and skip dbt result
                entityToProcess = {
                    ...entityToProcess,
                    urn: siblingUrn,
                };
            }
            if (entityPlatform === 'snowflake' && !isSiblingAheadInTheList) {
                // do nothing - render as normal
            }
            if (entityPlatform === 'snowflake' && isSiblingAheadInTheList) {
                // hide this because dbt will render above with snowflake urn
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

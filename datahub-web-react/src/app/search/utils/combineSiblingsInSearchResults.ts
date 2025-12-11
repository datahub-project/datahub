/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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

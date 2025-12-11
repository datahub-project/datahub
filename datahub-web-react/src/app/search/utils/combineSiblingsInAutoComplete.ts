/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { CombinedEntity, createSiblingEntityCombiner } from '@app/entity/shared/siblingUtils';

import { AutoCompleteResultForEntity, EntityType } from '@types';

export type CombinedSuggestion = {
    type: EntityType;
    combinedEntities: Array<CombinedEntity>;
    suggestions?: AutoCompleteResultForEntity['suggestions'];
};

export function combineSiblingsInAutoComplete(
    autoCompleteResultForEntity: AutoCompleteResultForEntity,
    { combineSiblings = false } = {},
): CombinedSuggestion {
    const combine = createSiblingEntityCombiner();
    const combinedEntities: Array<CombinedEntity> = [];

    autoCompleteResultForEntity.entities.forEach((entity) => {
        if (!combineSiblings) {
            combinedEntities.push({ entity });
            return;
        }
        const combinedResult = combine(entity);
        if (!combinedResult.skipped) combinedEntities.push(combinedResult.combinedEntity);
    });

    return {
        type: autoCompleteResultForEntity.type,
        suggestions: autoCompleteResultForEntity.suggestions,
        combinedEntities,
    };
}

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

export type EntityWithMatchedFields = {
    entity: Entity;
    matchedFields?: MatchedField[];
};

export type CombinedEntityWithMatchedFields = CombinedEntity & Pick<EntityWithMatchedFields, 'matchedFields'>;

export function combineSiblingsInEntitiesWithMatchedFields(
    entities: EntityWithMatchedFields[] | undefined,
    shouldSepareteSiblings: boolean,
): CombinedEntityWithMatchedFields[] {
    const combine = createSiblingEntityCombiner();
    const combinedSearchResults: CombinedEntityWithMatchedFields[] = [];

    entities?.forEach((entityWithMatchedFields) => {
        if (!shouldSepareteSiblings) {
            combinedSearchResults.push(entityWithMatchedFields);
            return;
        }

        const combinedResult = combine(entityWithMatchedFields.entity);
        if (!combinedResult.skipped) {
            combinedSearchResults.push({
                ...entityWithMatchedFields,
                ...combinedResult.combinedEntity,
            });
        }
    });

    return combinedSearchResults;
}

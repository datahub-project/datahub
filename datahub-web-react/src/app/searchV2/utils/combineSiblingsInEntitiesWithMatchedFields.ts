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

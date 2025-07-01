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

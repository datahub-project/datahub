import { AutoCompleteResultForEntity, EntityType } from '../../../types.generated';
import { CombinedEntity, createSiblingEntityCombiner } from '../../entity/shared/siblingUtils';

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
    const combinedResults: Array<CombinedEntity> = [];

    autoCompleteResultForEntity.entities.forEach((entity) => {
        if (!combineSiblings) {
            combinedResults.push({ entity });
            return;
        }
        const combinedEntity = combine(entity);
        if (combinedEntity) combinedResults.push(combinedEntity);
    });

    return {
        type: autoCompleteResultForEntity.type,
        suggestions: autoCompleteResultForEntity.suggestions,
        combinedEntities: combinedResults,
    };
}

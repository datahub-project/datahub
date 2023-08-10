import { AutoCompleteResultForEntity, EntityType } from '../../../types.generated';
import { CombineOptions, CombinedEntityResult, combineSiblingEntities } from '../../entity/shared/siblingUtils';

export type CombinedSuggestion = {
    type: EntityType;
    combinedEntities: Array<CombinedEntityResult>;
    suggestions?: AutoCompleteResultForEntity['suggestions'];
};

export function combineSiblingsInAutoComplete(
    input: AutoCompleteResultForEntity,
    { combine = true }: CombineOptions = {},
): CombinedSuggestion {
    const combinedSuggestion: CombinedSuggestion = {
        type: input.type,
        suggestions: input.suggestions,
        combinedEntities: combineSiblingEntities(input.entities, { combine }),
    };

    return combinedSuggestion;
}

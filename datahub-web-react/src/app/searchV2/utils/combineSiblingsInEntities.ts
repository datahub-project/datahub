import { CombinedEntity, createSiblingEntityCombiner } from '@app/entity/shared/siblingUtils';

import { Entity } from '@types';

// TODO:: add tests
export function combineSiblingsInEntities(
    entities: Entity[] | undefined,
    shouldSepareteSiblings: boolean,
): CombinedEntity[] {
    const combine = createSiblingEntityCombiner();
    const combinedSearchResults: CombinedEntity[] = [];

    entities?.forEach((entity) => {
        if (!shouldSepareteSiblings) {
            combinedSearchResults.push({ entity });
            return;
        }

        const combinedResult = combine(entity);
        if (!combinedResult.skipped) {
            combinedSearchResults.push({
                ...combinedResult.combinedEntity,
            });
        }
    });

    return combinedSearchResults;
}

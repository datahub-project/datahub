import { EntityType } from '@src/types.generated';

/**
 * Returns an array of entity types based on the provided entity type.
 *
 * If the entity type is 'Tag', the function returns an array with only 'Tag'.
 * Otherwise, it returns an array containing 'GlossaryNode' and 'GlossaryTerm'.
 *
 * @param entity - The entity type to evaluate.
 * @returns An array of entity types.
 */
export const getEntitiesByTagORTerm = (entity: EntityType): Array<EntityType> => {
    // If the provided entity is 'Tag', return an array with just 'Tag'
    return entity === EntityType.Tag
        ? [EntityType.Tag]
        : // Otherwise, return 'GlossaryNode' and 'GlossaryTerm'
          [EntityType.GlossaryNode, EntityType.GlossaryTerm];
};

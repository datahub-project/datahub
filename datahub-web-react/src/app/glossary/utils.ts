import { EntityType } from '../../types.generated';
import { GenericEntityProperties } from '../entity/shared/types';

export const ROOT_NODES = 'rootNodes';
export const ROOT_TERMS = 'rootTerms';

export function getGlossaryRootToUpdate(entityType: EntityType) {
    return entityType === EntityType.GlossaryTerm ? ROOT_TERMS : ROOT_NODES;
}

export function getParentNodeToUpdate(entityData: GenericEntityProperties | null, entityType: EntityType) {
    return entityData?.parentNodes?.nodes.length
        ? entityData?.parentNodes?.nodes[0].urn
        : getGlossaryRootToUpdate(entityType);
}

export function updateGlossarySidebar(
    parentNodesToUpdate: string[],
    updatedUrns: string[],
    setUpdatedUrns: (updatdUrns: string[]) => void,
) {
    setUpdatedUrns([...updatedUrns, ...parentNodesToUpdate]);
}

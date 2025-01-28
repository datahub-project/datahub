import { EntityType } from '../../types.generated';
import { GenericEntityProperties } from '../entity/shared/types';

export const ROOT_NODES = 'rootNodes';
export const ROOT_TERMS = 'rootTerms';

export function getGlossaryRootToUpdate(entityType: EntityType) {
    return entityType === EntityType.GlossaryTerm ? ROOT_TERMS : ROOT_NODES;
}

// Get the urns or special constants for root nodes or terms (above) that need to be refreshed in the Glossary
// sidebar when making updates (edit name, create term/term group, delete term/term group, move entity)
export function getParentNodeToUpdate(entityData: GenericEntityProperties | null, entityType: EntityType) {
    return entityData?.parentNodes?.nodes?.length
        ? entityData?.parentNodes?.nodes[0]?.urn
        : getGlossaryRootToUpdate(entityType);
}

// Add the parent nodes that need to refetch from the glossary sidebar to `urnsToUpdate` state.
// This could also include ROOT_NODES or ROOT_TERMS if the item(s) that need updating don't have parents.
export function updateGlossarySidebar(
    parentNodesToUpdate: string[],
    urnsToUpdate: string[],
    setUrnsToUpdate: (updatdUrns: string[]) => void,
) {
    setUrnsToUpdate([...urnsToUpdate, ...parentNodesToUpdate]);
}

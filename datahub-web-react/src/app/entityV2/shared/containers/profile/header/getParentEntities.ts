import { GenericEntityProperties } from '@app/entity/shared/types';

import { DataProduct, Entity, EntityType, ParentDocumentsResult } from '@types';

type GetContextPathInput = Pick<
    GenericEntityProperties,
    'parent' | 'parentContainers' | 'parentDomains' | 'parentNodes' | 'domain'
> & {
    parentDocuments?: ParentDocumentsResult;
};

const MAX_PARENT_DEPTH = 10;

/** Walk a nested `parent` chain (direct parent first). */
function collectNestedParents(parent: GenericEntityProperties | null | undefined): Entity[] {
    const parents: Entity[] = [];
    let current: GenericEntityProperties | null | undefined = parent;
    while (current?.urn && parents.length < MAX_PARENT_DEPTH) {
        parents.push(current as Entity);
        current = current.parent ?? undefined;
    }
    return parents;
}

export function getParentEntities(entityData: GetContextPathInput | null, entityType?: EntityType): Entity[] {
    if (!entityData) return [];

    switch (entityType) {
        case EntityType.DataProduct: {
            const domain = (entityData as DataProduct).domain?.domain;
            return domain ? [domain, ...(domain.parentDomains?.domains || [])] : [];
        }

        case EntityType.GlossaryTerm:
        case EntityType.GlossaryNode:
            return entityData.parentNodes?.nodes || [];

        case EntityType.Domain:
            return entityData.parentDomains?.domains || [];

        case EntityType.Document:
            return entityData.parentDocuments?.documents || [];

        default: {
            // generic fallback
            const containerPath =
                entityData.parentContainers?.containers ||
                entityData.parentDomains?.domains ||
                entityData.parentNodes?.nodes ||
                [];
            if (containerPath.length) return containerPath;

            if (entityData.parent) return collectNestedParents(entityData.parent);

            return [];
        }
    }
}

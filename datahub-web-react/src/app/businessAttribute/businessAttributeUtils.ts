import EntityRegistry from '../entity/EntityRegistry';
import { Entity, EntityType } from '../../types.generated';

export function sortBusinessAttributes(entityRegistry: EntityRegistry, nodeA?: Entity | null, nodeB?: Entity | null) {
    const nodeAName = entityRegistry.getDisplayName(EntityType.BusinessAttribute, nodeA) || '';
    const nodeBName = entityRegistry.getDisplayName(EntityType.BusinessAttribute, nodeB) || '';
    return nodeAName.localeCompare(nodeBName);
}

export function getRelatedEntitiesUrl(entityRegistry: EntityRegistry, urn: string) {
    return `${entityRegistry.getEntityUrl(EntityType.BusinessAttribute, urn)}/${encodeURIComponent(
        'Related Entities',
    )}`;
}

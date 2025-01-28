import { Entity, EntityType } from '../../../types.generated';
import { EntityRegistry } from '../../../entityRegistryContext';

export function sortGlossaryTerms(entityRegistry: EntityRegistry, nodeA?: Entity | null, nodeB?: Entity | null) {
    const nodeAName = entityRegistry.getDisplayName(EntityType.GlossaryTerm, nodeA) || '';
    const nodeBName = entityRegistry.getDisplayName(EntityType.GlossaryTerm, nodeB) || '';
    return nodeAName.localeCompare(nodeBName);
}

export function getRelatedEntitiesUrl(entityRegistry: EntityRegistry, urn: string) {
    return `${entityRegistry.getEntityUrl(EntityType.GlossaryTerm, urn)}/${encodeURIComponent('Related Entities')}`;
}

export function getRelatedAssetsUrl(entityRegistry: EntityRegistry, urn: string) {
    return `${entityRegistry.getEntityUrl(EntityType.GlossaryTerm, urn)}/${encodeURIComponent('Related Assets')}`;
}

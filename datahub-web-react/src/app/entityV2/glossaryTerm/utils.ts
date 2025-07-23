import { EntityRegistry } from '@src/entityRegistryContext';

import { Entity, EntityType, GlossaryTerm } from '@types';

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

export function isGlossaryTerm(entity?: Entity | null | undefined): entity is GlossaryTerm {
    return !!entity && entity.type === EntityType.GlossaryTerm;
}

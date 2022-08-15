import { Entity, EntityType } from '../../../types.generated';
import EntityRegistry from '../EntityRegistry';

export function sortGlossaryTerms(entityRegistry: EntityRegistry, nodeA?: Entity | null, nodeB?: Entity | null) {
    const nodeAName = entityRegistry.getDisplayName(EntityType.GlossaryTerm, nodeA) || '';
    const nodeBName = entityRegistry.getDisplayName(EntityType.GlossaryTerm, nodeB) || '';
    return nodeAName.localeCompare(nodeBName);
}

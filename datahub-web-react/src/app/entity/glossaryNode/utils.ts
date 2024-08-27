import { Entity, EntityType } from '../../../types.generated';
import EntityRegistry from '../EntityRegistry';

export function sortGlossaryNodes(entityRegistry: EntityRegistry, nodeA?: Entity | null, nodeB?: Entity | null) {
    const nodeAName = entityRegistry.getDisplayName(EntityType.GlossaryNode, nodeA);
    const nodeBName = entityRegistry.getDisplayName(EntityType.GlossaryNode, nodeB);
    return nodeAName.localeCompare(nodeBName);
}

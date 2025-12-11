/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { EntityRegistry } from '@src/entityRegistryContext';

import { Entity, EntityType, GlossaryNode } from '@types';

export function sortGlossaryNodes(entityRegistry: EntityRegistry, nodeA?: Entity | null, nodeB?: Entity | null) {
    const nodeAName = entityRegistry.getDisplayName(EntityType.GlossaryNode, nodeA) || '';
    const nodeBName = entityRegistry.getDisplayName(EntityType.GlossaryNode, nodeB) || '';
    return nodeAName.localeCompare(nodeBName);
}

export function isGlossaryNode(entity?: Entity | null | undefined): entity is GlossaryNode {
    return !!entity && entity.type === EntityType.GlossaryNode;
}

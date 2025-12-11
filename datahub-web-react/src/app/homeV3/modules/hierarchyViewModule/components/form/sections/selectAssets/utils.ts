/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { TreeNode } from '@app/homeV3/modules/hierarchyViewModule/treeView/types';

import { Entity } from '@types';

// Build tree by unwrapping parent entities
export function unwrapParentEntitiesToTreeNodes<T extends Entity>(
    items: T[] | undefined,
    parentEntitiesGetter: (item: T) => Entity[],
): TreeNode[] | undefined {
    if (items === undefined) return undefined;

    const treeNodes: TreeNode[] = [];
    items.forEach((item) => {
        const chainOfEntities = [...parentEntitiesGetter(item), item];

        let lastNode: TreeNode | undefined;

        chainOfEntities.forEach((entity) => {
            const foundNode = (lastNode?.children ?? treeNodes).find((node) => node.value === entity.urn);

            if (foundNode) {
                lastNode = foundNode;
            } else {
                const newNode: TreeNode = {
                    value: entity.urn,
                    label: entity.urn,
                    entity,
                };
                if (lastNode?.children) {
                    lastNode.children.push(newNode);
                } else if (lastNode) {
                    lastNode.children = [newNode];
                } else {
                    treeNodes.push(newNode);
                }
                lastNode = newNode;
            }
        });
    });
    return treeNodes;
}

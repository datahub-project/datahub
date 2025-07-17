import { TreeNode } from '@app/homeV3/modules/hierarchyViewModule/treeView/types';

import { EntityType } from '@types';

export function createNode(
    value: string,
    label: string,
    children?: TreeNode[],
    parentValue?: string,
    entityType: EntityType = EntityType.Dataset,
    hasAsyncChildren?: boolean,
): TreeNode {
    return {
        value,
        label,
        children,
        parentValue,
        hasAsyncChildren,
        entity: {
            type: entityType,
            urn: `urn:li:${entityType}:${value}`,
        },
    };
}

import { TreeNode } from '@app/homeV3/modules/hierarchyViewModule/treeView/types';

import { Entity } from '@types';

export function convertEntityToTreeNode(entity: Entity, parentValue?: string): TreeNode {
    return {
        // add parent value to avoid duplicated values as the same entity could be attached to different assets
        value: `${parentValue ?? ''} / ${entity.urn}`,
        label: entity.urn,
        entity,
    };
}

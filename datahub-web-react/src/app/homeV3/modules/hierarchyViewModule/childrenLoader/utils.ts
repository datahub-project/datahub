import { ChildrenLoaderMetadata } from '@app/homeV3/modules/hierarchyViewModule/childrenLoader/types';
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

// A missing total means its request failed (a successful or skipped load always records one), so report the
// count as unknown rather than 0; otherwise the tree would treat the node as having no children.
export function getTotalChildren(metadata: ChildrenLoaderMetadata): number | undefined {
    const { totalNumberOfChildren, totalNumberOfRelatedEntities } = metadata;
    if (totalNumberOfChildren === undefined || totalNumberOfRelatedEntities === undefined) return undefined;
    return totalNumberOfChildren + totalNumberOfRelatedEntities;
}

import {
    GlossaryNodeType,
    GlossaryTermType,
} from '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/glossaryTreeView/types';
import { unwrapParentEntitiesToTreeNodes } from '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/utils';
import { TreeNode } from '@app/homeV3/modules/hierarchyViewModule/treeView/types';

import { GlossaryNode, GlossaryTerm } from '@types';

export function convertGlossaryNodeToTreeNode(glossaryNode: GlossaryNodeType): TreeNode {
    const childrenNodesCount = glossaryNode.childrenCount?.nodesCount ?? 0;
    const childrenTermsCount = glossaryNode.childrenCount?.termsCount ?? 0;
    const childrenCount = childrenNodesCount + childrenTermsCount;

    return {
        value: glossaryNode.urn,
        label: glossaryNode.urn,
        hasAsyncChildren: childrenCount > 0,
        totalChildren: childrenCount,
        entity: glossaryNode,
    };
}

export function convertGlossaryTermToTreeNode(glossaryTerm: GlossaryTermType): TreeNode {
    return {
        value: glossaryTerm.urn,
        label: glossaryTerm.urn,
        entity: glossaryTerm,
    };
}

export function unwrapFlatGlossaryNodesToTreeNodes(glossaryItems: GlossaryNode[]): TreeNode[] {
    return unwrapParentEntitiesToTreeNodes(glossaryItems, (item) => [...(item.parentNodes?.nodes ?? [])].reverse());
}

export function unwrapFlatGlossaryTermsToTreeNodes(glossaryItems: GlossaryTerm[]): TreeNode[] {
    return unwrapParentEntitiesToTreeNodes(glossaryItems, (item) => [...(item.parentNodes?.nodes ?? [])].reverse());
}

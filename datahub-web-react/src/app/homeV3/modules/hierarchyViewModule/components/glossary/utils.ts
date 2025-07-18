import { unwrapParentEntitiesToTreeNodes } from '@app/homeV3/modules/hierarchyViewModule/components/form/sections/selectAssets/utils';
import { GlossaryNodeType, GlossaryTermType } from '@app/homeV3/modules/hierarchyViewModule/components/glossary/types';
import { TreeNode } from '@app/homeV3/modules/hierarchyViewModule/treeView/types';

import { GlossaryNode, GlossaryTerm } from '@types';

export function convertGlossaryNodeToTreeNode(glossaryNode: GlossaryNodeType, forceHasAsyncChildren = false): TreeNode {
    const childrenNodesCount = glossaryNode.childrenCount?.nodesCount ?? 0;
    const childrenTermsCount = glossaryNode.childrenCount?.termsCount ?? 0;
    const childrenCount = childrenNodesCount + childrenTermsCount;

    return {
        value: glossaryNode.urn,
        label: glossaryNode.urn,
        hasAsyncChildren: forceHasAsyncChildren || childrenCount > 0,
        totalChildren: childrenCount,
        entity: glossaryNode,
    };
}

export function convertGlossaryTermToTreeNode(glossaryTerm: GlossaryTermType, forceHasAsyncChildren = false): TreeNode {
    return {
        value: glossaryTerm.urn,
        label: glossaryTerm.urn,
        entity: glossaryTerm,
        hasAsyncChildren: forceHasAsyncChildren,
    };
}

export function unwrapFlatGlossaryNodesToTreeNodes(glossaryItems: GlossaryNode[] | undefined): TreeNode[] | undefined {
    return unwrapParentEntitiesToTreeNodes(glossaryItems, (item) => [...(item.parentNodes?.nodes ?? [])].reverse());
}

export function unwrapFlatGlossaryTermsToTreeNodes(glossaryItems: GlossaryTerm[] | undefined): TreeNode[] | undefined {
    return unwrapParentEntitiesToTreeNodes(glossaryItems, (item) => [...(item.parentNodes?.nodes ?? [])].reverse());
}

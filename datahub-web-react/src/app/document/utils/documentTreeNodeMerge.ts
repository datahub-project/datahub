import { DocumentTreeNode } from '@app/document/DocumentTreeContext';

/**
 * Merge a server-fetched child into an existing tree node.
 *
 * Server metadata wins for title/flags, but any already-loaded `children`
 * subtree is kept — otherwise re-fetching a parent wipes nested expand state
 * and the sidebar jumps empty.
 */
export function mergeServerChildNode(
    existing: DocumentTreeNode | undefined,
    serverChild: DocumentTreeNode,
): DocumentTreeNode {
    if (!existing) return serverChild;
    return {
        ...serverChild,
        children: existing.children !== undefined ? existing.children : serverChild.children,
        hasChildren: serverChild.hasChildren || existing.hasChildren,
        childCount: serverChild.childCount ?? existing.childCount,
    };
}

/**
 * True when expanding a node should hit the network for its first page of
 * children. `children === undefined` means never loaded; `[]` means loaded empty.
 */
export function shouldFetchChildrenOnExpand(node: DocumentTreeNode): boolean {
    return node.hasChildren && node.children === undefined;
}

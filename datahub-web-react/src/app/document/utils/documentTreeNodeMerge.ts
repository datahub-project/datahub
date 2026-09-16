import { DocumentTreeNode } from '@app/document/DocumentTreeContext';

/** Default title written at create time — search can lag behind local renames. */
export const DEFAULT_DOCUMENT_TITLE = 'New Document';

/**
 * Merge a server-fetched child into an existing tree node.
 *
 * Server metadata wins for flags, but:
 * - already-loaded `children` are kept (re-fetch must not wipe expand state)
 * - a local rename is kept when search still returns the create-time default title
 */
export function mergeServerChildNode(
    existing: DocumentTreeNode | undefined,
    serverChild: DocumentTreeNode,
): DocumentTreeNode {
    if (!existing) return serverChild;

    const serverTitleIsStaleDefault = !serverChild.title || serverChild.title === DEFAULT_DOCUMENT_TITLE;
    const keepLocalTitle = !!existing.title && existing.title !== serverChild.title && serverTitleIsStaleDefault;

    return {
        ...serverChild,
        title: keepLocalTitle ? existing.title : serverChild.title,
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

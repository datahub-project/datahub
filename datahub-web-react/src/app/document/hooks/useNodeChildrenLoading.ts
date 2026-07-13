import { useCallback, useState } from 'react';

import { DocumentTreeNode, useDocumentTree } from '@app/document/DocumentTreeContext';

interface NodeLoaders {
    /** Loads (and returns) the first page of a node's children. */
    loadChildren: (parentUrn: string | null) => Promise<DocumentTreeNode[]>;
    /** Loads the next page of an already-expanded node's children. */
    loadMoreChildren: (parentUrn: string) => Promise<void>;
}

interface NodeChildrenLoading {
    /** Nodes whose first page of children is being fetched (drives the row spinner). */
    loadingUrns: Set<string>;
    /** Nodes whose next page of children is being fetched (drives the load-more spinner). */
    loadingChildrenUrns: Set<string>;
    /** Toggle a single node: collapse if open, otherwise expand and fetch its children. */
    handleToggleExpand: (urn: string) => Promise<void>;
    /** Fetch the next page of children for an already-expanded parent. */
    handleLoadMoreChildren: (parentUrn: string) => Promise<void>;
}

/**
 * Per-node expand + lazy child-loading for the document sidebar tree.
 *
 * Owns the two "in flight" sets (first-page vs next-page) and the toggle/load-more
 * handlers, keeping that async bookkeeping out of the render-heavy tree component.
 * The loaders are passed in rather than pulled from `useLoadDocumentTree` here so
 * we don't spin up a second root-loading instance — the tree owns the single
 * loader and hands us the child fetches it already has.
 */
export function useNodeChildrenLoading({ loadChildren, loadMoreChildren }: NodeLoaders): NodeChildrenLoading {
    const { getNode, expandedUrns, expandNode, collapseNode } = useDocumentTree();
    const [loadingUrns, setLoadingUrns] = useState<Set<string>>(new Set());
    const [loadingChildrenUrns, setLoadingChildrenUrns] = useState<Set<string>>(new Set());

    const handleToggleExpand = useCallback(
        async (urn: string) => {
            const node = getNode(urn);
            if (!node) return;

            if (expandedUrns.has(urn)) {
                collapseNode(urn);
                return;
            }

            expandNode(urn);
            // Always refetch on expand; the context merges server data with any
            // optimistic local children rather than clobbering them.
            if (node.hasChildren) {
                setLoadingUrns((prev) => new Set(prev).add(urn));
                await loadChildren(urn);
                setLoadingUrns((prev) => {
                    const next = new Set(prev);
                    next.delete(urn);
                    return next;
                });
            }
        },
        [getNode, expandedUrns, expandNode, collapseNode, loadChildren],
    );

    const handleLoadMoreChildren = useCallback(
        async (parentUrn: string) => {
            setLoadingChildrenUrns((prev) => new Set(prev).add(parentUrn));
            await loadMoreChildren(parentUrn);
            setLoadingChildrenUrns((prev) => {
                const next = new Set(prev);
                next.delete(parentUrn);
                return next;
            });
        },
        [loadMoreChildren],
    );

    return { loadingUrns, loadingChildrenUrns, handleToggleExpand, handleLoadMoreChildren };
}

import { useCallback, useRef, useState } from 'react';

import { DocumentTreeNode, useDocumentTree } from '@app/document/DocumentTreeContext';
import { shouldFetchChildrenOnExpand } from '@app/document/utils/documentTreeNodeMerge';

interface NodeLoaders {
    /** Loads (and returns) the first page of a node's children. */
    loadChildren: (parentUrn: string | null) => Promise<DocumentTreeNode[]>;
    /** Loads the next page of an already-expanded node's children. */
    loadMoreChildren: (parentUrn: string) => Promise<DocumentTreeNode[] | void>;
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
 * Loaders are injected so DocumentTree keeps a single `useLoadDocumentTree`
 * instance (a second hook would re-fetch roots).
 */
export function useNodeChildrenLoading({ loadChildren, loadMoreChildren }: NodeLoaders): NodeChildrenLoading {
    const { getNode, expandedUrns, expandNode, collapseNode } = useDocumentTree();
    const [loadingUrns, setLoadingUrns] = useState<Set<string>>(new Set());
    const [loadingChildrenUrns, setLoadingChildrenUrns] = useState<Set<string>>(new Set());
    // Sync in-flight sets — React state alone can miss rapid double-clicks before re-render.
    const loadingUrnsRef = useRef<Set<string>>(new Set());
    const loadingChildrenUrnsRef = useRef<Set<string>>(new Set());

    const handleToggleExpand = useCallback(
        async (urn: string) => {
            const node = getNode(urn);
            if (!node) return;

            if (expandedUrns.has(urn)) {
                collapseNode(urn);
                return;
            }

            expandNode(urn);

            if (!shouldFetchChildrenOnExpand(node) || loadingUrnsRef.current.has(urn)) {
                return;
            }

            loadingUrnsRef.current.add(urn);
            setLoadingUrns(new Set(loadingUrnsRef.current));
            try {
                await loadChildren(urn);
            } finally {
                loadingUrnsRef.current.delete(urn);
                setLoadingUrns(new Set(loadingUrnsRef.current));
            }
        },
        [getNode, expandedUrns, expandNode, collapseNode, loadChildren],
    );

    const handleLoadMoreChildren = useCallback(
        async (parentUrn: string) => {
            if (loadingChildrenUrnsRef.current.has(parentUrn)) return;

            loadingChildrenUrnsRef.current.add(parentUrn);
            setLoadingChildrenUrns(new Set(loadingChildrenUrnsRef.current));
            try {
                await loadMoreChildren(parentUrn);
            } finally {
                loadingChildrenUrnsRef.current.delete(parentUrn);
                setLoadingChildrenUrns(new Set(loadingChildrenUrnsRef.current));
            }
        },
        [loadMoreChildren],
    );

    return { loadingUrns, loadingChildrenUrns, handleToggleExpand, handleLoadMoreChildren };
}

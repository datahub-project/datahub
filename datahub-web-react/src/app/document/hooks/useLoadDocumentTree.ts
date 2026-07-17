import { useCallback, useRef, useState } from 'react';

import { useInfiniteScroll } from '@components/components/InfiniteScrollList/useInfiniteScroll';

import { useUserContext } from '@app/context/useUserContext';
import { DocumentTreeNode, useDocumentTree } from '@app/document/DocumentTreeContext';
import { documentToTreeNode, sortDocumentsByCreationTime } from '@app/document/utils/documentUtils';

import { useSearchDocumentsLazyQuery } from '@graphql/document.generated';
import { Document } from '@types';

export const DOCUMENT_PAGE_SIZE = 25;

/**
 * Hook to load and populate the document tree from backend queries.
 *
 * Root documents use useInfiniteScroll for automatic pagination.
 * Children use manual pagination state per parent.
 */

export function useLoadDocumentTree() {
    const { initializeTree, appendRootNodes, setNodeChildren, appendNodeChildren, getRootNodes } = useDocumentTree();
    const [searchDocumentsQuery] = useSearchDocumentsLazyQuery();

    // Scope the document tree (overview page + sidebar) to the active View, mirroring
    // how the rest of search respects the selected View. The picker popovers rely on
    // useSearchDocuments' applyView opt-out for their search box, but the shared tree
    // itself is always View-scoped.
    const userContext = useUserContext();
    const viewUrn = userContext.localState?.selectedViewUrn ?? undefined;

    // True until the first page of root documents finishes loading.
    // ContextDocumentsPage depends on this to show a spinner before redirecting.
    const [isInitializing, setIsInitializing] = useState(true);

    // Per-parent child pagination state
    const childPaginationRef = useRef<Map<string, { offset: number; total: number }>>(new Map());
    const [childPaginationVersion, setChildPaginationVersion] = useState(0);

    const hasMoreChildren = useCallback(
        (parentUrn: string) => {
            // eslint-disable-next-line @typescript-eslint/no-unused-expressions
            childPaginationVersion; // Subscribe to changes
            const state = childPaginationRef.current.get(parentUrn);
            if (!state) return false;
            return state.offset < state.total;
        },
        [childPaginationVersion],
    );

    // Check if multiple documents have children (batch query)
    const checkForChildren = useCallback(
        async (urns: string[]): Promise<Record<string, boolean>> => {
            if (urns.length === 0) return {};

            try {
                const result = await searchDocumentsQuery({
                    variables: {
                        input: {
                            query: '*',
                            parentDocuments: urns,
                            start: 0,
                            count: urns.length * 100,
                            viewUrn,
                        },
                    },
                    fetchPolicy: 'network-only',
                });

                const childrenMap: Record<string, boolean> = {};
                urns.forEach((urn) => {
                    childrenMap[urn] = false;
                });

                const children = result.data?.searchDocuments?.documents || [];

                children.forEach((child) => {
                    const parentUrn = child.info?.parentDocument?.document?.urn;
                    if (parentUrn && Object.prototype.hasOwnProperty.call(childrenMap, parentUrn)) {
                        childrenMap[parentUrn] = true;
                    }
                });

                return childrenMap;
            } catch (error) {
                console.error('Failed to check for children:', error);
                return {};
            }
        },
        [searchDocumentsQuery, viewUrn],
    );

    // fetchData for useInfiniteScroll — fetches root documents and pushes into tree context
    const fetchRootDocuments = useCallback(
        async (start: number, _count: number): Promise<DocumentTreeNode[]> => {
            try {
                const result = await searchDocumentsQuery({
                    variables: {
                        input: {
                            query: '*',
                            rootOnly: true,
                            // No sourceType filter — sidebar now shows native + external docs.
                            // Source filtering is applied client-side per platform via the sidebar filters.
                            start,
                            count: DOCUMENT_PAGE_SIZE,
                            viewUrn,
                        },
                    },
                    fetchPolicy: start === 0 ? 'cache-and-network' : 'network-only',
                });

                const documents = (result.data?.searchDocuments?.documents || []) as Document[];

                const sorted = sortDocumentsByCreationTime(documents);
                const childUrns = sorted.map((d) => d.urn);
                const hasChildrenMap = await checkForChildren(childUrns);
                const nodes = sorted.map((d) => documentToTreeNode(d, hasChildrenMap[d.urn] || false));

                if (start === 0) {
                    const currentRoots = getRootNodes();
                    if (currentRoots.length === 0) {
                        initializeTree(nodes);
                    }
                } else {
                    appendRootNodes(nodes);
                }

                return nodes;
            } catch (error) {
                console.error('Failed to load root documents:', error);
                return [];
            } finally {
                if (start === 0) setIsInitializing(false);
            }
        },
        [searchDocumentsQuery, checkForChildren, initializeTree, appendRootNodes, getRootNodes, viewUrn],
    );

    const {
        loading: loadingRoots,
        observerRef: rootObserverRef,
        hasMore: hasMoreRoots,
    } = useInfiniteScroll<DocumentTreeNode>({
        fetchData: fetchRootDocuments,
        pageSize: DOCUMENT_PAGE_SIZE,
        getKey: (node) => node.urn,
    });

    // Load children for a specific parent (first page, called on expand)
    const loadChildren = useCallback(
        async (parentUrn: string | null) => {
            try {
                const result = await searchDocumentsQuery({
                    variables: {
                        input: {
                            query: '*',
                            // rootOnly avoids returning every document when loading the tree root.
                            parentDocuments: parentUrn ? [parentUrn] : undefined,
                            rootOnly: parentUrn === null ? true : undefined,
                            start: 0,
                            count: DOCUMENT_PAGE_SIZE,
                            viewUrn,
                        },
                    },
                    fetchPolicy: 'network-only',
                });

                const documents = (result.data?.searchDocuments?.documents || []) as Document[];
                const total = result.data?.searchDocuments?.total || 0;

                const sortedDocuments = sortDocumentsByCreationTime(documents);
                const childUrns = sortedDocuments.map((doc) => doc.urn);
                const hasChildrenMap = await checkForChildren(childUrns);

                const treeNodes: DocumentTreeNode[] = sortedDocuments.map((doc) =>
                    documentToTreeNode(doc, hasChildrenMap[doc.urn] || false),
                );

                setNodeChildren(parentUrn, treeNodes);

                if (parentUrn) {
                    childPaginationRef.current.set(parentUrn, {
                        offset: documents.length,
                        total,
                    });
                    setChildPaginationVersion((v) => v + 1);
                }

                return treeNodes;
            } catch (error) {
                console.error('Failed to load children:', error);
                return [];
            }
        },
        [searchDocumentsQuery, checkForChildren, setNodeChildren, viewUrn],
    );

    // Load more children for a parent (subsequent pages)
    const loadMoreChildren = useCallback(
        async (parentUrn: string) => {
            const state = childPaginationRef.current.get(parentUrn);
            if (!state || state.offset >= state.total) return;

            try {
                const result = await searchDocumentsQuery({
                    variables: {
                        input: {
                            query: '*',
                            parentDocuments: [parentUrn],
                            start: state.offset,
                            count: DOCUMENT_PAGE_SIZE,
                            viewUrn,
                        },
                    },
                    fetchPolicy: 'network-only',
                });

                const documents = (result.data?.searchDocuments?.documents || []) as Document[];
                const sorted = sortDocumentsByCreationTime(documents);
                const childUrns = sorted.map((d) => d.urn);
                const hasChildrenMap = await checkForChildren(childUrns);
                const nodes = sorted.map((d) => documentToTreeNode(d, hasChildrenMap[d.urn] || false));

                appendNodeChildren(parentUrn, nodes);

                childPaginationRef.current.set(parentUrn, {
                    offset: state.offset + documents.length,
                    total: state.total,
                });
                setChildPaginationVersion((v) => v + 1);
            } catch (error) {
                console.error('Failed to load more children:', error);
            }
        },
        [searchDocumentsQuery, checkForChildren, appendNodeChildren, viewUrn],
    );

    return {
        loadChildren,
        loadMoreChildren,
        checkForChildren,
        loading: isInitializing,
        loadingMoreRoots: loadingRoots && !isInitializing,
        hasMoreRoots,
        hasMoreChildren,
        rootObserverRef,
    };
}

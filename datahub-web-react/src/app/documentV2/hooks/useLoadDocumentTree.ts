import { useApolloClient } from '@apollo/client';
import { useCallback, useEffect } from 'react';

import { DocumentTreeNode, useDocumentTree } from '@app/documentV2/DocumentTreeContext';
import { useSearchDocuments } from '@app/documentV2/hooks/useSearchDocuments';

import { SearchDocumentsDocument } from '@graphql/document.generated';
import { Document, DocumentState } from '@types';

/**
 * Hook to load and populate the document tree from backend queries.
 *
 * Handles:
 * - Initial load of root documents
 * - Checking if documents have children
 * - Loading children on demand
 */

function documentToTreeNode(doc: Document, hasChildren: boolean): DocumentTreeNode {
    return {
        urn: doc.urn,
        title: doc.info?.title || 'Untitled',
        parentUrn: doc.info?.parentDocument?.document?.urn || null,
        hasChildren,
        children: undefined, // Not loaded yet
    };
}

export function useLoadDocumentTree() {
    const { initializeTree, setNodeChildren, getRootNodes } = useDocumentTree();
    const apolloClient = useApolloClient();

    // Load root documents
    const { documents: rootDocuments, loading: loadingRoot } = useSearchDocuments({
        query: '*',
        rootOnly: true,
        states: [DocumentState.Published, DocumentState.Unpublished],
        includeDrafts: false,
        start: 0,
        count: 100,
        fetchPolicy: 'cache-first',
    });

    // Check if multiple documents have children (batch query)
    const checkForChildren = useCallback(
        async (urns: string[]): Promise<Record<string, boolean>> => {
            if (urns.length === 0) return {};

            console.log('🔍 checkForChildren called with urns:', urns);

            try {
                const result = await apolloClient.query({
                    query: SearchDocumentsDocument,
                    variables: {
                        input: {
                            query: '*',
                            parentDocuments: urns, // Batch query
                            states: [DocumentState.Published, DocumentState.Unpublished],
                            includeDrafts: false,
                            start: 0,
                            count: urns.length * 100,
                        },
                    },
                    fetchPolicy: 'network-only', // Always fetch fresh to check for children
                });

                console.log('📦 checkForChildren raw result:', result);

                const childrenMap: Record<string, boolean> = {};
                urns.forEach((urn) => {
                    childrenMap[urn] = false;
                });

                const children = result.data?.searchDocuments?.documents || [];
                console.log('👶 checkForChildren found children:', children.length);

                children.forEach((child) => {
                    const parentUrn = child.info?.parentDocument?.document?.urn;
                    console.log('  - Child', child.urn, 'has parent:', parentUrn);
                    if (parentUrn && childrenMap.hasOwnProperty(parentUrn)) {
                        childrenMap[parentUrn] = true;
                    }
                });

                console.log('✅ checkForChildren result:', childrenMap);
                return childrenMap;
            } catch (error) {
                console.error('❌ Failed to check for children:', error);
                return {};
            }
        },
        [apolloClient],
    );

    // Load children for a specific parent
    const loadChildren = useCallback(
        async (parentUrn: string | null) => {
            try {
                const result = await apolloClient.query({
                    query: SearchDocumentsDocument,
                    variables: {
                        input: {
                            query: '*',
                            parentDocument: parentUrn,
                            states: [DocumentState.Published, DocumentState.Unpublished],
                            includeDrafts: false,
                            start: 0,
                            count: 100,
                        },
                    },
                    fetchPolicy: 'cache-first', // Can use cache for children
                });

                const documents = result.data?.searchDocuments?.documents || [];

                // Sort by creation time (most recent first)
                const sortedDocuments = [...documents].sort((a, b) => {
                    const timeA = a.info?.created?.time || 0;
                    const timeB = b.info?.created?.time || 0;
                    return timeB - timeA; // DESC order
                });

                // Check if these documents have children
                const childUrns = sortedDocuments.map((doc) => doc.urn);
                const hasChildrenMap = await checkForChildren(childUrns);

                // Convert to tree nodes
                const treeNodes: DocumentTreeNode[] = sortedDocuments.map((doc) =>
                    documentToTreeNode(doc, hasChildrenMap[doc.urn] || false),
                );

                // Update tree state
                setNodeChildren(parentUrn, treeNodes);

                return treeNodes;
            } catch (error) {
                console.error('Failed to load children:', error);
                return [];
            }
        },
        [apolloClient, checkForChildren, setNodeChildren],
    );

    // Initialize tree with root documents on mount (ONLY if tree is empty)
    useEffect(() => {
        if (!loadingRoot && rootDocuments.length > 0) {
            const initializeAsync = async () => {
                // Check if tree is already initialized (without making it a dependency)
                const currentRootNodes = getRootNodes();
                const isTreeEmpty = currentRootNodes.length === 0;

                if (isTreeEmpty) {
                    console.log('🌳 Initializing document tree with', rootDocuments.length, 'root documents');

                    // Sort root documents by creation time (most recent first)
                    const sortedRootDocuments = [...rootDocuments].sort((a, b) => {
                        const timeA = a.info?.created?.time || 0;
                        const timeB = b.info?.created?.time || 0;
                        return timeB - timeA; // DESC order
                    });

                    // Check which root documents have children
                    const rootDocUrns = sortedRootDocuments.map((doc) => doc.urn);
                    const hasChildrenMap = await checkForChildren(rootDocUrns);

                    // Convert to tree nodes
                    const rootNodes: DocumentTreeNode[] = sortedRootDocuments.map((doc) =>
                        documentToTreeNode(doc, hasChildrenMap[doc.urn] || false),
                    );

                    initializeTree(rootNodes);
                    console.log('✅ Tree initialized (first time only)');
                } else {
                    console.log('ℹ️ Tree already initialized, skipping initialization');
                }
            };

            initializeAsync();
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [loadingRoot, rootDocuments]);

    return {
        loadChildren,
        checkForChildren,
        loading: loadingRoot,
    };
}

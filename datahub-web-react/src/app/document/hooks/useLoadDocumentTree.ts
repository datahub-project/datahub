import { useApolloClient } from '@apollo/client';
import { useCallback, useEffect } from 'react';

import { DocumentTreeNode, useDocumentTree } from '@app/document/DocumentTreeContext';
import { useSearchDocuments } from '@app/document/hooks/useSearchDocuments';
import { documentToTreeNode, sortDocumentsByCreationTime } from '@app/document/utils/documentUtils';

import { SearchDocumentsDocument } from '@graphql/document.generated';
import { DocumentSourceType } from '@types';

/**
 * Hook to load and populate the document tree from backend queries.
 *
 * Handles:
 * - Initial load of root documents
 * - Checking if documents have children
 * - Loading children on demand
 */

export function useLoadDocumentTree() {
    const { initializeTree, setNodeChildren, getRootNodes } = useDocumentTree();
    const apolloClient = useApolloClient();

    // Load root documents
    const { documents: rootDocuments, loading: loadingRoot } = useSearchDocuments({
        query: '*',
        rootOnly: true,
        start: 0,
        count: 100,
        fetchPolicy: 'cache-first',
        sourceTypes: [DocumentSourceType.Native],
    });

    // Check if multiple documents have children (batch query)
    // TODO: Consider refactoring to use useLazyQuery for better type safety
    // once Apollo is updated to support returning data directly from lazy queries
    const checkForChildren = useCallback(
        async (urns: string[]): Promise<Record<string, boolean>> => {
            if (urns.length === 0) return {};

            try {
                const result = await apolloClient.query({
                    query: SearchDocumentsDocument,
                    variables: {
                        input: {
                            query: '*',
                            parentDocuments: urns, // Batch query
                            start: 0,
                            count: urns.length * 100,
                        },
                    },
                    fetchPolicy: 'network-only', // Always fetch fresh to check for children
                });

                const childrenMap: Record<string, boolean> = {};
                urns.forEach((urn) => {
                    childrenMap[urn] = false;
                });

                const children = result.data?.searchDocuments?.documents || [];

                children.forEach((child) => {
                    const parentUrn = child.info?.parentDocument?.document?.urn;
                    if (parentUrn && childrenMap.hasOwnProperty(parentUrn)) {
                        childrenMap[parentUrn] = true;
                    }
                });

                return childrenMap;
            } catch (error) {
                console.error('Failed to check for children:', error);
                return {};
            }
        },
        [apolloClient],
    );

    // Load children for a specific parent
    // TODO: Consider refactoring to use useLazyQuery for better type safety
    // once Apollo is updated to support returning data directly from lazy queries
    const loadChildren = useCallback(
        async (parentUrn: string | null) => {
            try {
                const result = await apolloClient.query({
                    query: SearchDocumentsDocument,
                    variables: {
                        input: {
                            query: '*',
                            parentDocuments: parentUrn ? [parentUrn] : undefined,
                            start: 0,
                            count: 100,
                        },
                    },
                    fetchPolicy: 'cache-first', // Can use cache for children
                });

                const documents = result.data?.searchDocuments?.documents || [];

                // Sort by creation time (most recent first)
                const sortedDocuments = sortDocumentsByCreationTime(documents);

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
                    // Sort root documents by creation time (most recent first)
                    const sortedRootDocuments = sortDocumentsByCreationTime(rootDocuments);

                    // Check which root documents have children
                    const rootDocUrns = sortedRootDocuments.map((doc) => doc.urn);
                    const hasChildrenMap = await checkForChildren(rootDocUrns);

                    // Convert to tree nodes
                    const rootNodes: DocumentTreeNode[] = sortedRootDocuments.map((doc) =>
                        documentToTreeNode(doc, hasChildrenMap[doc.urn] || false),
                    );

                    initializeTree(rootNodes);
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

import { useApolloClient } from '@apollo/client';
import { useCallback } from 'react';

import { SearchDocumentsDocument } from '@graphql/document.generated';
import { DocumentState } from '@types';

export interface DocumentChild {
    urn: string;
    title: string;
}

// Pagination constants
const CHILD_PAGE_SIZE = 100; // Fetch up to 100 children per parent level

export function useDocumentChildren() {
    const client = useApolloClient();

    /**
     * Check if any of the given parent documents have children
     * Returns a map of parentUrn -> hasChildren
     */
    const checkForChildren = useCallback(
        async (parentUrns: string[]): Promise<Record<string, boolean>> => {
            if (parentUrns.length === 0) {
                return {};
            }

            try {
                console.log('Checking', parentUrns.length, 'parents for children in ONE batch query');

                // Initialize all parents as having no children
                const childrenMap: Record<string, boolean> = {};
                parentUrns.forEach((urn) => {
                    childrenMap[urn] = false;
                });

                // Make ONE batch query for all children of all parents
                const result = await client.query({
                    query: SearchDocumentsDocument,
                    variables: {
                        input: {
                            query: '*',
                            parentDocuments: parentUrns, // Batch query with all parents
                            states: [DocumentState.Published, DocumentState.Unpublished],
                            includeDrafts: false,
                            start: 0,
                            count: CHILD_PAGE_SIZE * parentUrns.length, // Fetch enough for all parents
                        },
                    },
                    fetchPolicy: 'network-only',
                });

                // Group children by their parent URN
                const children = result.data?.searchDocuments?.documents || [];
                children.forEach((child) => {
                    const parentUrn = child.info?.parentDocument?.document?.urn;
                    if (parentUrn && childrenMap.hasOwnProperty(parentUrn)) {
                        childrenMap[parentUrn] = true;
                    }
                });

                const parentsWithChildren = Object.values(childrenMap).filter(Boolean).length;
                console.log(
                    `Found ${parentsWithChildren} parents with children out of ${parentUrns.length} total (batch query)`,
                );

                return childrenMap;
            } catch (error) {
                console.error('Failed to check for children:', error);
                return {};
            }
        },
        [client],
    );

    /**
     * Fetch all children for a specific parent document
     */
    const fetchChildren = useCallback(
        async (parentUrn: string): Promise<DocumentChild[]> => {
            try {
                console.log('Fetching children for parent:', parentUrn);

                const result = await client.query({
                    query: SearchDocumentsDocument,
                    variables: {
                        input: {
                            query: '*',
                            parentDocument: parentUrn,
                            states: [DocumentState.Published, DocumentState.Unpublished],
                            includeDrafts: false,
                            start: 0,
                            count: CHILD_PAGE_SIZE, // Limit children per level
                        },
                    },
                    fetchPolicy: 'network-only', // Always fetch fresh data
                });

                console.log('Fetch children result:', result);

                if (!result || result.error || result.errors) {
                    console.error('Failed to fetch children:', result?.error || result?.errors);
                    return [];
                }

                const { data } = result;

                const documents = data?.searchDocuments?.documents || [];
                return documents.map((doc) => ({
                    urn: doc.urn,
                    title: doc.info?.title || 'New Document',
                }));
            } catch (error) {
                console.error('Failed to fetch children:', error);
                return [];
            }
        },
        [client],
    );

    return {
        checkForChildren,
        fetchChildren,
        loading: false, // We manage loading state in the tree component
    };
}

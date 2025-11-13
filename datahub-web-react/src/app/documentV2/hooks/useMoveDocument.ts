import { useApolloClient } from '@apollo/client';
import { message } from 'antd';
import { useCallback } from 'react';

import { useDocumentsContext } from '@app/documentV2/DocumentsContext';
import { addDocumentToSearchCache, removeDocumentFromSearchCache } from '@app/documentV2/cacheUtils';

import { SearchDocumentsDocument, useMoveDocumentMutation } from '@graphql/document.generated';
import { DocumentState } from '@types';

export interface MoveDocumentInput {
    urn: string;
    parentDocument?: string | null;
    oldParentDocument?: string | null; // Track old parent for cache invalidation
}

export function useMoveDocument() {
    const apolloClient = useApolloClient();
    const { setUpdatedDocument, removeOptimisticDocument } = useDocumentsContext();
    const [moveDocumentMutation, { loading }] = useMoveDocumentMutation();

    const moveDocument = useCallback(
        async (input: MoveDocumentInput) => {
            console.log('📦 useMoveDocument called:', input);
            try {
                // First, try to read the document from the old parent's search cache
                // This gives us the document structure to add to the new parent's cache
                let documentFromCache: any = null;
                try {
                    const oldParentCacheKey = input.oldParentDocument
                        ? {
                              query: '*',
                              parentDocument: input.oldParentDocument,
                              states: [DocumentState.Published, DocumentState.Unpublished],
                              includeDrafts: false,
                              start: 0,
                              count: 100,
                          }
                        : {
                              query: '*',
                              rootOnly: true,
                              states: [DocumentState.Published, DocumentState.Unpublished],
                              includeDrafts: false,
                              start: 0,
                              count: 100,
                          };

                    const cachedData = apolloClient.readQuery({
                        query: SearchDocumentsDocument,
                        variables: { input: oldParentCacheKey },
                    });

                    documentFromCache = cachedData?.searchDocuments?.documents?.find(
                        (doc: any) => doc.urn === input.urn,
                    );
                    console.log('📄 Document from old parent cache:', documentFromCache ? 'FOUND' : 'NOT FOUND');
                } catch (e) {
                    console.log('⚠️ Could not read from old parent cache:', e);
                }

                const result = await moveDocumentMutation({
                    variables: {
                        input: {
                            urn: input.urn,
                            parentDocument: input.parentDocument || null,
                        },
                    },
                });

                console.log('✅ Move mutation result:', {
                    success: !!result.data?.moveDocument,
                    hasErrors: !!result.errors,
                });

                if (result.data?.moveDocument) {
                    // Remove from old parent's search cache (optimistic update)
                    removeDocumentFromSearchCache(apolloClient, input.urn, input.oldParentDocument);

                    // Add to new parent's search cache (optimistic update)
                    // This ensures immediate UI update without waiting for Elasticsearch
                    if (documentFromCache) {
                        // Update the parentDocument field in the cached document
                        const updatedDocument = {
                            ...documentFromCache,
                            info: {
                                ...documentFromCache.info,
                                parentDocument: input.parentDocument
                                    ? {
                                          document: {
                                              urn: input.parentDocument,
                                              info: { title: null, __typename: 'DocumentInfo' },
                                              __typename: 'Document',
                                          },
                                          __typename: 'ParentDocument',
                                      }
                                    : null,
                            },
                        };

                        addDocumentToSearchCache(apolloClient, updatedDocument, input.parentDocument);
                        console.log('✅ Added document to new parent cache');
                    } else {
                        console.warn('⚠️ Could not add to new parent cache - document not found in old cache');
                    }

                    // Remove from optimistic documents if it was optimistically created
                    removeOptimisticDocument(input.urn);

                    // Signal to DocumentTree to clear local caches and trigger refetch for expanded parents
                    // The refetch ensures we're in sync after Elasticsearch catches up
                    setUpdatedDocument({
                        urn: input.urn,
                        parentDocument: input.parentDocument || null,
                        oldParentDocument: input.oldParentDocument,
                    });

                    message.success('Moved Document');
                    return true;
                }

                throw new Error('Failed to move document');
            } catch (error) {
                console.error('Failed to move document:', error);
                message.error('Failed to move document');
                return false;
            }
        },
        [apolloClient, moveDocumentMutation, setUpdatedDocument, removeOptimisticDocument],
    );

    return {
        moveDocument,
        loading,
    };
}

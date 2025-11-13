import { useApolloClient } from '@apollo/client';
import { useCallback } from 'react';
import { useHistory } from 'react-router-dom';

import { useDocumentsContext } from '@app/documentV2/DocumentsContext';
import { addDocumentToSearchCache } from '@app/documentV2/cacheUtils';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useCreateDocumentMutation } from '@graphql/document.generated';
import { DocumentState, EntityType } from '@types';

export interface CreateDocumentInput {
    title?: string;
    subType?: string;
    state?: DocumentState;
    contents?: string;
    parentDocument?: string;
}

export function useCreateDocument() {
    const apolloClient = useApolloClient();
    const history = useHistory();
    const entityRegistry = useEntityRegistry();
    const { addOptimisticDocument } = useDocumentsContext();
    const [createDocumentMutation, { loading }] = useCreateDocumentMutation();

    const createDocument = useCallback(
        async (input: CreateDocumentInput) => {
            const title = input.title !== undefined ? input.title : '';
            const parentDocument = input.parentDocument || undefined;

            try {
                const result = await createDocumentMutation({
                    variables: {
                        input: {
                            title,
                            subType: input.subType,
                            state: input.state || DocumentState.Unpublished,
                            contents: {
                                text: input.contents || '',
                            },
                            parentDocument,
                        },
                    },
                });

                if (result.data?.createDocument) {
                    const documentUrn = result.data.createDocument;

                    // Construct a minimal document object for the cache
                    const newDocument = {
                        __typename: 'Document',
                        urn: documentUrn,
                        type: EntityType.Document,
                        subType: input.subType || null,
                        info: {
                            __typename: 'DocumentInfo',
                            title,
                            status: {
                                __typename: 'DocumentStatus',
                                state: input.state || DocumentState.Unpublished,
                            },
                            created: {
                                __typename: 'AuditStamp',
                                time: Date.now(),
                                actor: null,
                            },
                            lastModified: {
                                __typename: 'AuditStamp',
                                time: Date.now(),
                                actor: null,
                            },
                            parentDocument: parentDocument
                                ? {
                                      __typename: 'ParentDocument',
                                      document: {
                                          __typename: 'Document',
                                          urn: parentDocument,
                                          info: {
                                              __typename: 'DocumentInfo',
                                              title: null,
                                          },
                                      },
                                  }
                                : null,
                        },
                    };

                    // Update Apollo cache: Add to parent's children
                    addDocumentToSearchCache(apolloClient, newDocument, parentDocument);

                    // Add optimistic document with the REAL URN so it matches the URL
                    // This provides immediate visual feedback and will be deduplicated with real data
                    // No need to manually remove it - the merge logic in DocumentTree handles duplicates
                    addOptimisticDocument({
                        urn: documentUrn,
                        title,
                        parentDocument,
                        createdAt: Date.now(),
                    });

                    // Navigate to the newly created document
                    const url = entityRegistry.getEntityUrl(EntityType.Document, documentUrn);
                    history.push(url);

                    return documentUrn;
                }

                throw new Error('Failed to create document');
            } catch (error) {
                console.error('Failed to create document:', error);
                return null;
            }
        },
        [apolloClient, createDocumentMutation, history, entityRegistry, addOptimisticDocument],
    );

    return {
        createDocument,
        loading,
    };
}

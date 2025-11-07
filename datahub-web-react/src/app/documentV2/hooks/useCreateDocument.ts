import { useCallback } from 'react';
import { useHistory } from 'react-router-dom';

import { useDocumentsContext } from '@app/documentV2/DocumentsContext';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useCreateDocumentMutation } from '@graphql/document.generated';
import { DocumentState, EntityType } from '@types';

export interface CreateDocumentInput {
    title?: string;
    subType: string;
    state?: DocumentState;
    contents?: string;
    parentDocument?: string;
}

export function useCreateDocument() {
    const history = useHistory();
    const entityRegistry = useEntityRegistry();
    const { setNewDocument, addOptimisticDocument, removeOptimisticDocument } = useDocumentsContext();
    const [createDocumentMutation, { loading }] = useCreateDocumentMutation();

    const createDocument = useCallback(
        async (input: CreateDocumentInput) => {
            const title = input.title || 'New Document';
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

                    // Add optimistic document with the REAL URN so it matches the URL
                    addOptimisticDocument({
                        urn: documentUrn,
                        title,
                        parentDocument,
                        createdAt: Date.now(),
                    });

                    // Navigate to the newly created document
                    const url = entityRegistry.getEntityUrl(EntityType.Document, documentUrn);
                    history.push(url);

                    // Notify context that a new document was created
                    setNewDocument({
                        urn: documentUrn,
                        parentDocument,
                    });

                    // Remove optimistic document after the refetch completes
                    // The refetch happens 5 seconds after newDocument is set
                    setTimeout(() => {
                        removeOptimisticDocument(documentUrn);
                    }, 6000);

                    return documentUrn;
                }

                throw new Error('Failed to create document');
            } catch (error) {
                console.error('Failed to create document:', error);
                return null;
            }
        },
        [
            createDocumentMutation,
            history,
            entityRegistry,
            setNewDocument,
            addOptimisticDocument,
            removeOptimisticDocument,
        ],
    );

    return {
        createDocument,
        loading,
    };
}

import { message } from 'antd';
import { useCallback } from 'react';
import { useHistory } from 'react-router-dom';

import { useDocumentsContext } from '@app/documentV2/DocumentsContext';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useDeleteDocumentMutation } from '@graphql/document.generated';
import { EntityType } from '@types';

export interface DeleteDocumentInput {
    urn: string;
    parentUrn?: string | null;
}

export function useDeleteDocument() {
    const history = useHistory();
    const entityRegistry = useEntityRegistry();
    const { setDeletedDocument } = useDocumentsContext();
    const [deleteDocumentMutation, { loading }] = useDeleteDocumentMutation();

    const deleteDocument = useCallback(
        async (input: DeleteDocumentInput) => {
            try {
                const result = await deleteDocumentMutation({
                    variables: {
                        urn: input.urn,
                    },
                });

                if (result.data?.deleteDocument) {
                    // Notify context that a document was deleted
                    setDeletedDocument({
                        urn: input.urn,
                    });

                    message.success('Document deleted');

                    // Navigate to parent document or home
                    if (input.parentUrn) {
                        // Navigate to parent document
                        const url = entityRegistry.getEntityUrl(EntityType.Document, input.parentUrn);
                        history.push(url);
                    } else {
                        // Navigate to home page
                        history.push('/');
                    }

                    return true;
                }

                throw new Error('Failed to delete document');
            } catch (error) {
                console.error('Failed to delete document:', error);
                message.error('Failed to delete document');
                return false;
            }
        },
        [deleteDocumentMutation, setDeletedDocument, history, entityRegistry],
    );

    return {
        deleteDocument,
        loading,
    };
}


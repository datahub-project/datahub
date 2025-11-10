import { message } from 'antd';
import { useCallback } from 'react';

import { useDocumentsContext } from '@app/documentV2/DocumentsContext';

import { useMoveDocumentMutation } from '@graphql/document.generated';

export interface MoveDocumentInput {
    urn: string;
    parentDocument?: string | null;
}

export function useMoveDocument() {
    const { setUpdatedDocument } = useDocumentsContext();
    const [moveDocumentMutation, { loading }] = useMoveDocumentMutation();

    const moveDocument = useCallback(
        async (input: MoveDocumentInput) => {
            try {
                const result = await moveDocumentMutation({
                    variables: {
                        input: {
                            urn: input.urn,
                            parentDocument: input.parentDocument || null,
                        },
                    },
                });

                if (result.data?.moveDocument) {
                    // Notify context that a document was updated
                    setUpdatedDocument({
                        urn: input.urn,
                        parentDocument: input.parentDocument || null,
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
        [moveDocumentMutation, setUpdatedDocument],
    );

    return {
        moveDocument,
        loading,
    };
}

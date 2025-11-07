import { useCallback } from 'react';

import {
    useUpdateDocumentContentsMutation,
    useUpdateDocumentRelatedEntitiesMutation,
    useUpdateDocumentStatusMutation,
} from '@graphql/document.generated';
import { DocumentState } from '@types';

export interface UpdateDocumentContentsInput {
    urn: string;
    title?: string;
    contents?: { text: string };
    subType?: string;
}

export interface UpdateDocumentStatusInput {
    urn: string;
    state: DocumentState;
}

export interface UpdateDocumentRelatedEntitiesInput {
    urn: string;
    relatedAssets?: string[];
    relatedDocuments?: string[];
}

export function useUpdateDocument() {
    const [updateContentsMutation, { loading: updatingContents }] = useUpdateDocumentContentsMutation();
    const [updateStatusMutation, { loading: updatingStatus }] = useUpdateDocumentStatusMutation();
    const [updateRelatedEntitiesMutation, { loading: updatingRelatedEntities }] =
        useUpdateDocumentRelatedEntitiesMutation();

    const updateContents = useCallback(
        async (input: UpdateDocumentContentsInput) => {
            try {
                const variables: any = {
                    urn: input.urn,
                    title: input.title,
                    subType: input.subType,
                };

                if (input.contents) {
                    variables.contents = input.contents;
                }

                const result = await updateContentsMutation({
                    variables: {
                        input: variables,
                    },
                });

                if (result.data?.updateDocumentContents) {
                    return true;
                }

                throw new Error('Failed to update document');
            } catch (error) {
                console.error('Failed to update document:', error);
                return false;
            }
        },
        [updateContentsMutation],
    );

    const updateStatus = useCallback(
        async (input: UpdateDocumentStatusInput) => {
            try {
                const result = await updateStatusMutation({
                    variables: {
                        input: {
                            urn: input.urn,
                            state: input.state,
                        },
                    },
                });

                if (result.data?.updateDocumentStatus) {
                    return true;
                }

                throw new Error('Failed to update document status');
            } catch (error) {
                console.error('Failed to update document status:', error);
                return false;
            }
        },
        [updateStatusMutation],
    );

    const updateRelatedEntities = useCallback(
        async (input: UpdateDocumentRelatedEntitiesInput) => {
            try {
                const result = await updateRelatedEntitiesMutation({
                    variables: {
                        input: {
                            urn: input.urn,
                            relatedAssets: input.relatedAssets,
                            relatedDocuments: input.relatedDocuments,
                        },
                    },
                });

                if (result.data?.updateDocumentRelatedEntities) {
                    return true;
                }

                throw new Error('Failed to update related entities');
            } catch (error) {
                console.error('Failed to update related entities:', error);
                // Silent fail - don't show error message for this operation
                return false;
            }
        },
        [updateRelatedEntitiesMutation],
    );

    return {
        updateContents,
        updateStatus,
        updateRelatedEntities,
        loading: updatingContents || updatingStatus || updatingRelatedEntities,
    };
}

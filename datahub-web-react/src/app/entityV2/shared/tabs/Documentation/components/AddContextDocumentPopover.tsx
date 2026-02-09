import { useApolloClient } from '@apollo/client';
import { message } from 'antd';
import React, { useCallback, useState } from 'react';
import styled from 'styled-components';

import { useUpdateDocument } from '@app/document/hooks/useUpdateDocument';
import { createDefaultDocumentInput, extractRelatedAssetUrns, mergeUrns } from '@app/document/utils/documentUtils';
import { DocumentPopoverBase } from '@app/homeV2/layout/sidebar/documents/shared/DocumentPopoverBase';
import { Button } from '@src/alchemy-components';
import { colors } from '@src/alchemy-components/theme';

import { GetDocumentDocument, useCreateDocumentMutation } from '@graphql/document.generated';
import { DocumentSourceType } from '@types';

const NewDocumentButton = styled(Button)`
    width: 100%;
    justify-content: start;
    color: ${colors.gray[1700]};
    &:hover {
        background: linear-gradient(
            180deg,
            rgba(243, 244, 246, 0.5) -3.99%,
            rgba(235, 236, 240, 0.5) 53.04%,
            rgba(235, 236, 240, 0.5) 100%
        );
        box-shadow: 0px 0px 0px 1px rgba(139, 135, 157, 0.08);
    }
    padding: 12px 8px;
`;

interface AddContextDocumentPopoverProps {
    /** The URN of the current entity to link documents to */
    entityUrn: string;
    /** Callback when a document is selected/created and modal should be opened */
    onDocumentSelected: (documentUrn: string) => void;
    /** Callback when popover should close */
    onClose: () => void;
}

/**
 * Popover for adding context documents to an entity.
 * Allows users to:
 * - Select an existing document (updates its relatedAssets)
 * - Create a new document at root level
 * - Create a new document as a child of any existing document
 */
export const AddContextDocumentPopover: React.FC<AddContextDocumentPopoverProps> = ({
    entityUrn,
    onDocumentSelected,
    onClose,
}) => {
    const [isCreating, setIsCreating] = useState(false);
    const apolloClient = useApolloClient();
    const [createDocumentMutation] = useCreateDocumentMutation();
    const { updateRelatedEntities } = useUpdateDocument();

    /**
     * Handle selecting an existing document - fetch current relatedAssets and merge
     */
    const handleSelectExistingDocument = useCallback(
        async (documentUrn: string) => {
            setIsCreating(true);
            try {
                // Fetch the document to get current relatedAssets
                const { data } = await apolloClient.query({
                    query: GetDocumentDocument,
                    variables: { urn: documentUrn, includeParentDocuments: false },
                    fetchPolicy: 'cache-first', // Use cache if available, otherwise fetch
                });

                const document = data?.document;
                if (!document) {
                    throw new Error('Document not found');
                }

                // Extract existing related asset URNs and merge with entity URN
                const existingAssetUrns = extractRelatedAssetUrns(document);
                const mergedAssetUrns = mergeUrns(existingAssetUrns, [entityUrn]);

                // Update document with merged relatedAssets
                const success = await updateRelatedEntities({
                    urn: documentUrn,
                    relatedAssets: mergedAssetUrns,
                });

                if (success) {
                    // Open the document in modal
                    onDocumentSelected(documentUrn);
                    onClose();
                } else {
                    message.error('Failed to link document. Please try again.');
                    // Keep popover open on error
                }
            } catch (error) {
                console.error('Failed to update document related assets:', error);
                message.error('Failed to link document. Please try again.');
                // Keep popover open on error
            } finally {
                setIsCreating(false);
            }
        },
        [entityUrn, apolloClient, updateRelatedEntities, onDocumentSelected, onClose],
    );

    /**
     * Handle creating a new document
     */
    const handleCreateDocument = useCallback(
        async (parentUrn: string | null = null) => {
            setIsCreating(true);
            try {
                // Create document with relatedAssets directly in the mutation
                const result = await createDocumentMutation({
                    variables: {
                        input: createDefaultDocumentInput({
                            parentUrn,
                            relatedAssetUrns: [entityUrn],
                        }),
                    },
                });

                const newDocumentUrn = result.data?.createDocument;
                if (!newDocumentUrn) {
                    throw new Error('Failed to create document');
                }

                // Open the new document in modal
                onDocumentSelected(newDocumentUrn);
                onClose();
            } catch (error) {
                console.error('Failed to create document:', error);
                message.error('Failed to create document. Please try again.');
                // Keep popover open on error
            } finally {
                setIsCreating(false);
            }
        },
        [entityUrn, createDocumentMutation, onDocumentSelected, onClose],
    );

    const headerContent = (
        <NewDocumentButton
            icon={{ icon: 'Plus', source: 'phosphor' }}
            variant="text"
            onClick={() => handleCreateDocument(null)}
            disabled={isCreating}
            data-testid="new-document-root-button"
        >
            New document
        </NewDocumentButton>
    );

    return (
        <DocumentPopoverBase
            headerContent={headerContent}
            onSelectDocument={handleSelectExistingDocument}
            onSelectSearchResult={handleSelectExistingDocument}
            onCreateChild={handleCreateDocument}
            hideActions={false}
            hideActionsMenu
            maxHeight={400}
            searchDisabled={isCreating}
            // Search all document types (both native and external/ingested)
            // to allow linking documents from third-party sources like Notion
            sourceTypes={[DocumentSourceType.Native, DocumentSourceType.External]}
        />
    );
};

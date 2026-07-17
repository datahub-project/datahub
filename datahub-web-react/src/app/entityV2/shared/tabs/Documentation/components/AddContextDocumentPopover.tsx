import { useApolloClient } from '@apollo/client';
import { toast } from '@components';
import { Plus } from '@phosphor-icons/react/dist/csr/Plus';
import React, { useCallback, useMemo, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { useUpdateDocument } from '@app/document/hooks/useUpdateDocument';
import {
    computeRelatedEntitiesForLinkChange,
    createDefaultDocumentInput,
    extractRelatedAssetUrns,
    extractRelatedDocumentUrns,
} from '@app/document/utils/documentUtils';
import { DocumentPopoverBase } from '@app/homeV2/layout/sidebar/documents/shared/DocumentPopoverBase';
import { Button } from '@src/alchemy-components';

import { GetDocumentDocument, useCreateDocumentMutation } from '@graphql/document.generated';
import { DocumentSourceType } from '@types';

const NewDocumentButton = styled(Button)`
    width: 100%;
    justify-content: start;
    color: ${(props) => props.theme.colors.textSecondary};
    &:hover {
        background: ${(props) => props.theme.colors.bgHover};
        box-shadow: ${(props) => props.theme.colors.shadowFocus};
    }
    padding: 12px 8px;
`;

interface AddContextDocumentPopoverProps {
    /** The URN of the current entity to link documents to */
    entityUrn: string;
    /** Callback when a document is created and its modal should be opened */
    onDocumentSelected: (documentUrn: string) => void;
    /** Callback fired after a document's link state changes so the caller can refetch. */
    onDocumentsLinked?: () => void;
    /** Callback when popover should close */
    onClose: () => void;
    /**
     * URNs of documents already linked to the entity. These render pre-checked, and
     * form the baseline that Save diffs against to decide what to link/unlink.
     */
    linkedDocumentUrns?: string[];
}

/**
 * Popover for managing which context documents are linked to an entity.
 *
 * The checkboxes are a view of the entity's Resources: already-linked docs open
 * pre-checked, and toggling a box stages an add/remove without persisting. Saving
 * diffs the staged selection against what was linked and applies every add + remove
 * in one pass, then refetches and closes. Creating a new document (root or child)
 * links it to the entity and opens it in the editor.
 */
export const AddContextDocumentPopover: React.FC<AddContextDocumentPopoverProps> = ({
    entityUrn,
    onDocumentSelected,
    onDocumentsLinked,
    onClose,
    linkedDocumentUrns,
}) => {
    const { t } = useTranslation('entity.profile.documentation');
    const [isCreating, setIsCreating] = useState(false);
    const [isSaving, setIsSaving] = useState(false);
    // The set of docs linked when the popover opened — the baseline we diff against
    // on Save. Captured once so a background refetch can't shift the baseline mid-edit.
    const initialUrns = useRef<Set<string>>(new Set(linkedDocumentUrns ?? []));
    // Staged selection: seeded from the baseline, toggled locally, persisted on Save.
    const [checkedUrns, setCheckedUrns] = useState<Set<string>>(() => new Set(linkedDocumentUrns ?? []));
    const apolloClient = useApolloClient();
    const [createDocumentMutation] = useCreateDocumentMutation();
    const { updateRelatedEntities } = useUpdateDocument();

    const handleToggleDocument = useCallback((documentUrn: string, isNowChecked: boolean) => {
        setCheckedUrns((prev) => {
            const next = new Set(prev);
            if (isNowChecked) next.add(documentUrn);
            else next.delete(documentUrn);
            return next;
        });
    }, []);

    // Diff the staged selection against the baseline to find what to link / unlink.
    const { addedUrns, removedUrns } = useMemo(() => {
        const added = [...checkedUrns].filter((urn) => !initialUrns.current.has(urn));
        const removed = [...initialUrns.current].filter((urn) => !checkedUrns.has(urn));
        return { addedUrns: added, removedUrns: removed };
    }, [checkedUrns]);

    const hasChanges = addedUrns.length > 0 || removedUrns.length > 0;

    /**
     * Fetch a document and set its link to the current entity to `shouldBeLinked`.
     * A document can be linked to a regular entity (relatedAssets) or to another
     * document (relatedDocuments); we edit whichever list the current entity belongs
     * to and leave the other untouched. Returns whether the write succeeded.
     */
    const applyLink = useCallback(
        async (documentUrn: string, shouldBeLinked: boolean): Promise<boolean> => {
            try {
                const { data } = await apolloClient.query({
                    query: GetDocumentDocument,
                    variables: { urn: documentUrn, includeParentDocuments: false },
                    fetchPolicy: 'network-only',
                });

                const document = data?.document;
                if (!document) return false;

                const { relatedAssets, relatedDocuments } = computeRelatedEntitiesForLinkChange({
                    entityUrn,
                    existingAssetUrns: extractRelatedAssetUrns(document),
                    existingRelatedDocumentUrns: extractRelatedDocumentUrns(document),
                    shouldBeLinked,
                });

                return await updateRelatedEntities({
                    urn: documentUrn,
                    relatedAssets,
                    relatedDocuments,
                });
            } catch (error) {
                console.error('Failed to update document link', documentUrn, error);
                return false;
            }
        },
        [entityUrn, apolloClient, updateRelatedEntities],
    );

    const handleSave = useCallback(async () => {
        if (!hasChanges) return;
        setIsSaving(true);
        const results = await Promise.all([
            ...addedUrns.map((urn) => applyLink(urn, true)),
            ...removedUrns.map((urn) => applyLink(urn, false)),
        ]);
        const failureCount = results.filter((ok) => !ok).length;
        const successCount = results.length - failureCount;

        if (failureCount > 0) {
            toast.error(t('failedToLinkDocument'));
        }
        // Reset the spinner before unmounting on success so we never set state on an
        // unmounted component (onClose tears this popover down).
        setIsSaving(false);
        if (successCount > 0) {
            toast.success(t('resourcesUpdated'));
            onDocumentsLinked?.();
            onClose();
        }
    }, [hasChanges, addedUrns, removedUrns, applyLink, onDocumentsLinked, onClose, t]);

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
                toast.error(t('failedToCreateDocument'));
                // Keep popover open on error
            } finally {
                setIsCreating(false);
            }
        },
        [entityUrn, createDocumentMutation, onDocumentSelected, onClose, t],
    );

    const headerContent = (
        <NewDocumentButton
            icon={{ icon: Plus }}
            variant="text"
            onClick={() => handleCreateDocument(null)}
            disabled={isCreating}
            data-testid="new-document-root-button"
        >
            {t('newDocument')}
        </NewDocumentButton>
    );

    return (
        <DocumentPopoverBase
            headerContent={headerContent}
            onCreateChild={handleCreateDocument}
            hideActions={false}
            hideActionsMenu
            maxHeight={400}
            searchDisabled={isCreating}
            // Search all document types (both native and external/ingested)
            // to allow linking documents from third-party sources like Notion
            sourceTypes={[DocumentSourceType.Native, DocumentSourceType.External]}
            multiSelect
            checkedUrns={checkedUrns}
            onToggleUrn={handleToggleDocument}
            onSave={handleSave}
            saveDisabled={!hasChanges}
            isSaving={isSaving}
        />
    );
};

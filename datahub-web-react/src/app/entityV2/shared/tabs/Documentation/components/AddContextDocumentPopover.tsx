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
import {
    ApplyLinkResult,
    DocumentLinkChanges,
    getDocumentLinkChanges,
    getSuccessfulDocumentLinkChanges,
} from '@app/entityV2/shared/tabs/Documentation/components/AddContextDocumentPopover.utils';
import { DocumentPopoverBase } from '@app/homeV2/layout/sidebar/documents/shared/DocumentPopoverBase';
import { Button } from '@src/alchemy-components';

import { GetDocumentDocument, useCreateDocumentMutation } from '@graphql/document.generated';
import { Document, DocumentSourceType } from '@types';

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

type AddContextDocumentPopoverProps = {
    entityUrn: string;
    onDocumentSelected: (documentUrn: string) => void;
    onDocumentsChanged?: (changes: DocumentLinkChanges) => void;
    onClose: () => void;
    linkedDocumentUrns?: string[];
};

/**
 * Popover for managing which context documents are linked to an entity.
 *
 * The checkboxes are a view of the entity's Resources: already-linked docs open
 * pre-checked, and toggling a box stages an add/remove without persisting. Saving
 * applies the staged changes, reports successful documents/removals for local list
 * updates, and closes. Creating a new document links it to the entity and opens it
 * in the editor.
 */
export const AddContextDocumentPopover: React.FC<AddContextDocumentPopoverProps> = ({
    entityUrn,
    onDocumentSelected,
    onDocumentsChanged,
    onClose,
    linkedDocumentUrns,
}) => {
    const { t } = useTranslation('entity.profile.documentation');
    const [isCreating, setIsCreating] = useState(false);
    const [isSaving, setIsSaving] = useState(false);
    const isSavingRef = useRef(false);
    const initialUrns = useRef<Set<string>>(new Set(linkedDocumentUrns ?? []));
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

    const changes = useMemo(() => getDocumentLinkChanges(initialUrns.current, checkedUrns), [checkedUrns]);
    const { addedUrns, removedUrns } = changes;

    const hasChanges = addedUrns.length > 0 || removedUrns.length > 0;

    /**
     * Fetch a document and set its link to the current entity to `shouldBeLinked`.
     * A document can be linked to a regular entity (relatedAssets) or to another
     * document (relatedDocuments); we edit whichever list the current entity belongs
     * to and leave the other untouched. Returns the write result and, on a successful
     * add, the document so Resources can render it without waiting on search.
     */
    const applyLink = useCallback(
        async (documentUrn: string, shouldBeLinked: boolean): Promise<ApplyLinkResult> => {
            try {
                const { data } = await apolloClient.query({
                    query: GetDocumentDocument,
                    variables: { urn: documentUrn, includeParentDocuments: false },
                    fetchPolicy: 'network-only',
                });

                const document = (data?.document as Document | null | undefined) ?? null;
                if (!document) return { ok: false, document: null };

                const { relatedAssets, relatedDocuments } = computeRelatedEntitiesForLinkChange({
                    entityUrn,
                    existingAssetUrns: extractRelatedAssetUrns(document),
                    existingRelatedDocumentUrns: extractRelatedDocumentUrns(document),
                    shouldBeLinked,
                });

                const ok = await updateRelatedEntities({
                    urn: documentUrn,
                    relatedAssets,
                    relatedDocuments,
                });
                return { ok, document: ok && shouldBeLinked ? document : null };
            } catch (error) {
                console.error('Failed to update document link', documentUrn, error);
                return { ok: false, document: null };
            }
        },
        [entityUrn, apolloClient, updateRelatedEntities],
    );

    const handleSave = useCallback(async () => {
        if (!hasChanges || isSavingRef.current) return;
        isSavingRef.current = true;
        setIsSaving(true);
        const [addedResults, removedResults] = await Promise.all([
            Promise.all(addedUrns.map((urn) => applyLink(urn, true))),
            Promise.all(removedUrns.map((urn) => applyLink(urn, false))),
        ]);
        const successfulChanges = getSuccessfulDocumentLinkChanges(addedResults, removedUrns, removedResults);
        const failureCount =
            addedResults.filter((result) => !result.ok).length + removedResults.filter((result) => !result.ok).length;
        const successCount = successfulChanges.addedDocuments.length + successfulChanges.removedUrns.length;

        if (failureCount > 0) {
            toast.error(t('failedToLinkDocument'));
        }
        isSavingRef.current = false;
        setIsSaving(false);
        if (successCount > 0) {
            toast.success(t('resourcesUpdated'));
            onDocumentsChanged?.(successfulChanges);
            onClose();
        }
    }, [hasChanges, addedUrns, removedUrns, applyLink, onDocumentsChanged, onClose, t]);

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

                setIsCreating(false);
                onDocumentSelected(newDocumentUrn);
                onClose();
            } catch (error) {
                console.error('Failed to create document:', error);
                toast.error(t('failedToCreateDocument'));
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

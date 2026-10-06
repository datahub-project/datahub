import { Plus } from '@phosphor-icons/react/dist/csr/Plus';
import React, { useCallback, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { useDocumentPermissions } from '@app/document/hooks/useDocumentPermissions';
import { useEntityData } from '@app/entity/shared/EntityContext';
import { EntityCapabilityType } from '@app/entityV2/Entity';
import { DocumentModal } from '@app/entityV2/document/DocumentModal';
import AddLinkModalUpdated from '@app/entityV2/shared/components/links/AddLinkModal';
import { EditLinkModal } from '@app/entityV2/shared/components/links/EditLinkModal';
import { useLinkListActions } from '@app/entityV2/shared/components/links/useLinkListActions';
import { AddContextDocumentPopover } from '@app/entityV2/shared/tabs/Documentation/components/AddContextDocumentPopover';
import RelatedResourcesPreview from '@app/entityV2/shared/tabs/Documentation/components/RelatedResourcesPreview';
import {
    combineAndSortRelatedItems,
    createRelatedSectionMenuItems,
    hasRelatedContent,
} from '@app/entityV2/shared/tabs/Documentation/components/relatedSectionUtils';
import { useResourcesDocuments } from '@app/entityV2/shared/tabs/Documentation/components/useResourcesDocuments';
import { useLinkPermission } from '@app/entityV2/summary/links/useLinkPermission';
import { ConfirmationModal } from '@app/sharedV2/modals/ConfirmationModal';
import { useIsContextDocumentsEnabled } from '@app/useAppConfig';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { Button, Menu, Pill, Popover, Tooltip } from '@src/alchemy-components';

const SectionContainer = styled.div`
    margin: 0 16px;
    padding: 0px;
`;

const SectionHeader = styled.div`
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 0px;
    user-select: none;
`;

const HeaderLeft = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
    min-width: 0;
`;

const HeaderRight = styled.div`
    display: flex;
    align-items: center;
    gap: 2px;
    flex-shrink: 0;
`;

const SectionTitle = styled.h3`
    font-size: 14px;
    font-weight: 600;
    color: ${(props) => props.theme.colors.text};
    margin: 0;
`;

const EmptyState = styled.div`
    font-size: 12px;
    color: ${(props) => props.theme.colors.textTertiary};
    padding: 8px 0;
`;

export const RelatedSection: React.FC = () => {
    const { t } = useTranslation('entity.profile.documentation');
    const { t: ta } = useTranslation('common.actions');
    const { urn, entityData, entityType } = useEntityData();
    const entityRegistry = useEntityRegistry();
    const [isAddLinkModalVisible, setIsAddLinkModalVisible] = useState(false);
    const [selectedDocumentUrn, setSelectedDocumentUrn] = useState<string | null>(null);
    const [showAddContextPopover, setShowAddContextPopover] = useState(false);

    const links = useMemo(
        () => entityData?.institutionalMemory?.elements || [],
        [entityData?.institutionalMemory?.elements],
    );
    const { editingMetadata, isEditModalOpen, onEdit, onCloseEditModal, handleDeleteLink } = useLinkListActions();

    // Check permissions and feature flags
    const hasLinkPermissions = useLinkPermission();
    const isContextDocumentsEnabled = useIsContextDocumentsEnabled();
    const { canCreate: canCreateDocuments } = useDocumentPermissions();
    // Trash-button gate. Backend enforces the real rule (EDIT_ENTITY_DOCS/EDIT_ENTITY on
    // the doc, or MANAGE_DOCUMENTS platform priv) — this is a best-effort UI-side check
    // that avoids showing the affordance to viewers with no plausible edit rights.
    const canRemoveDocuments = !!(
        entityData?.privileges?.canEditDescription || entityData?.privileges?.canManageEntity
    );

    // Fetch related documents if entity supports the capability
    const supportedCapabilities = entityType ? entityRegistry.getSupportedEntityCapabilities(entityType) : new Set();
    const supportsRelatedDocuments = supportedCapabilities.has(EntityCapabilityType.RELATED_DOCUMENTS);

    const {
        visibleDocuments,
        loading: documentsLoading,
        error: documentsError,
        documentUrnToRemove,
        requestRemove: setDocumentUrnToRemove,
        cancelRemove: cancelRemoveDocument,
        confirmRemove: handleConfirmRemoveDocument,
        handleDocumentsChanged,
        handleDocumentCreated,
        handleDocumentDeleted,
    } = useResourcesDocuments({
        entityUrn: urn,
        removeSuccessMessage: t('removeDocumentSuccess'),
        removeErrorMessage: t('removeDocumentError'),
    });

    const handleDocumentClick = useCallback((documentUrn: string) => {
        setSelectedDocumentUrn(documentUrn);
    }, []);

    const handleAddLink = useCallback(() => {
        setIsAddLinkModalVisible(true);
    }, []);

    const handleDocumentModalDeleted = useCallback(() => {
        if (!selectedDocumentUrn) return;
        handleDocumentDeleted(selectedDocumentUrn);
    }, [handleDocumentDeleted, selectedDocumentUrn]);

    const handleAddContext = useCallback(() => {
        setShowAddContextPopover(true);
    }, []);

    const handleDocumentSelected = useCallback(
        (documentUrn: string) => {
            setSelectedDocumentUrn(documentUrn);
            setShowAddContextPopover(false);
            handleDocumentCreated(documentUrn);
        },
        [handleDocumentCreated],
    );

    // Create menu items with feature flag and permission checks
    const menuItems = useMemo(
        () =>
            createRelatedSectionMenuItems({
                onAddLink: handleAddLink,
                onAddContext: handleAddContext,
                isContextDocumentsEnabled,
                hasLinkPermissions,
                canCreateDocuments,
            }),
        [handleAddLink, handleAddContext, isContextDocumentsEnabled, hasLinkPermissions, canCreateDocuments],
    );

    const hasLinks = links.length > 0;
    const hasDocuments = supportsRelatedDocuments && visibleDocuments.length > 0 && !documentsError;
    const hasContent = hasRelatedContent(hasLinks, hasDocuments);

    // Docs already in this section render pre-checked in the picker so users can
    // add/remove by toggling. Using `visibleDocuments` includes local add/remove
    // so the picker matches what the pills show.
    const linkedDocumentUrns = useMemo(
        () => (hasDocuments ? visibleDocuments.map((d) => d.urn) : []),
        [hasDocuments, visibleDocuments],
    );

    // Keep the preview order consistent with the full Resources modal.
    const sortedItems = useMemo(
        () => combineAndSortRelatedItems(links, hasDocuments ? visibleDocuments : null),
        [links, hasDocuments, visibleDocuments],
    );

    const itemCount = sortedItems.length;
    return (
        <>
            <SectionContainer>
                <SectionHeader data-testid="resources-section-header">
                    <HeaderLeft>
                        <SectionTitle>{t('resources')}</SectionTitle>
                        {itemCount > 0 && (
                            <Pill
                                label={String(itemCount)}
                                size="sm"
                                color="gray"
                                variant="filled"
                                dataTestId="resources-count-pill"
                            />
                        )}
                    </HeaderLeft>
                    <HeaderRight>
                        {supportsRelatedDocuments && menuItems.length > 0 && (
                            <Popover
                                open={showAddContextPopover}
                                trigger="click"
                                onOpenChange={(visible) => !visible && setShowAddContextPopover(false)}
                                content={
                                    urn && showAddContextPopover && !documentsLoading ? (
                                        <AddContextDocumentPopover
                                            entityUrn={urn}
                                            onDocumentSelected={handleDocumentSelected}
                                            onDocumentsChanged={handleDocumentsChanged}
                                            onClose={() => setShowAddContextPopover(false)}
                                            linkedDocumentUrns={linkedDocumentUrns}
                                        />
                                    ) : null
                                }
                                placement="bottomRight"
                                overlayStyle={{ padding: 0 }}
                                overlayInnerStyle={{
                                    padding: 0,
                                    background: 'transparent',
                                    boxShadow: 'none',
                                }}
                            >
                                <Menu items={menuItems} placement="bottomRight">
                                    <Tooltip title={t('addRelatedLinkTooltip')}>
                                        <Button
                                            variant="text"
                                            isCircle
                                            icon={{ icon: Plus }}
                                            aria-label={t('addRelatedLinkTooltip')}
                                            data-testid="add-related-button"
                                        />
                                    </Tooltip>
                                </Menu>
                            </Popover>
                        )}
                    </HeaderRight>
                </SectionHeader>

                <RelatedResourcesPreview
                    items={sortedItems}
                    canRemoveDocuments={canRemoveDocuments}
                    showMoreLabel={(count) => ta('showCountMoreCapitalized', { count })}
                    onDocumentClick={handleDocumentClick}
                    onDocumentRemove={setDocumentUrnToRemove}
                    onLinkEdit={onEdit}
                    onLinkDelete={handleDeleteLink}
                />

                {!hasContent && !documentsLoading && <EmptyState>{t('noRelatedLinks')}</EmptyState>}
            </SectionContainer>

            {isEditModalOpen && <EditLinkModal link={editingMetadata} onClose={onCloseEditModal} />}
            {isAddLinkModalVisible && (
                <AddLinkModalUpdated setShowAddLinkModal={(show) => setIsAddLinkModalVisible(show)} />
            )}
            {selectedDocumentUrn && (
                <DocumentModal
                    documentUrn={selectedDocumentUrn}
                    onClose={() => setSelectedDocumentUrn(null)}
                    onDocumentDeleted={handleDocumentModalDeleted}
                />
            )}
            <ConfirmationModal
                isOpen={documentUrnToRemove !== null}
                handleClose={cancelRemoveDocument}
                handleConfirm={handleConfirmRemoveDocument}
                modalTitle={t('removeDocumentConfirmTitle')}
                modalText={t('removeDocumentConfirmText')}
                confirmButtonText={ta('remove')}
                isDeleteModal
            />
        </>
    );
};

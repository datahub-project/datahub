import { CaretDown } from '@phosphor-icons/react/dist/csr/CaretDown';
import { CaretRight } from '@phosphor-icons/react/dist/csr/CaretRight';
import { Plus } from '@phosphor-icons/react/dist/csr/Plus';
import React, { useCallback, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { useDocumentPermissions } from '@app/document/hooks/useDocumentPermissions';
import { useRelatedDocuments } from '@app/document/hooks/useRelatedDocuments';
import { useEntityData } from '@app/entity/shared/EntityContext';
import { EntityCapabilityType } from '@app/entityV2/Entity';
import { DocumentModal } from '@app/entityV2/document/DocumentModal';
import AddLinkModalUpdated from '@app/entityV2/shared/components/links/AddLinkModal';
import { EditLinkModal } from '@app/entityV2/shared/components/links/EditLinkModal';
import { useLinkUtils } from '@app/entityV2/shared/components/links/useLinkUtils';
import { AddContextDocumentPopover } from '@app/entityV2/shared/tabs/Documentation/components/AddContextDocumentPopover';
import { ResourceDocumentPill } from '@app/entityV2/shared/tabs/Documentation/components/ResourceDocumentPill';
import { ResourceLinkPill } from '@app/entityV2/shared/tabs/Documentation/components/ResourceLinkPill';
import {
    RelatedItem,
    combineAndSortRelatedItems,
    createRelatedSectionMenuItems,
    hasRelatedContent,
} from '@app/entityV2/shared/tabs/Documentation/components/relatedSectionUtils';
import { useRemoveDocumentFromResources } from '@app/entityV2/shared/tabs/Documentation/components/useRemoveDocumentFromResources';
import { useResourcesCollapseState } from '@app/entityV2/shared/tabs/Documentation/components/useResourcesCollapseState';
import { useLinkPermission } from '@app/entityV2/summary/links/useLinkPermission';
import { ConfirmationModal } from '@app/sharedV2/modals/ConfirmationModal';
import { useIsContextDocumentsEnabled } from '@app/useAppConfig';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { Button, Menu, Pill, Popover, Text, Tooltip } from '@src/alchemy-components';

import { InstitutionalMemoryMetadata } from '@types';

const SectionHeader = styled.div`
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-top: 16px;
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

const SectionTitle = styled(Text)`
    font-weight: 700;
    font-size: 12px;
`;

const ListContainer = styled.div`
    display: flex;
    flex-wrap: wrap;
    gap: 6px;
    margin-top: 8px;
`;

const EmptyState = styled.div`
    font-size: 12px;
    color: ${(props) => props.theme.colors.textTertiary};
    padding: 8px 0;
`;

interface RelatedSectionProps {
    hideLinksButton?: boolean;
}

export default function RelatedSection({ hideLinksButton }: RelatedSectionProps) {
    const { t } = useTranslation('entity.profile.summary');
    const { t: ta } = useTranslation('common.actions');
    const { urn, entityData, entityType } = useEntityData();
    const entityRegistry = useEntityRegistry();
    const [isAddLinkModalVisible, setIsAddLinkModalVisible] = useState(false);
    const [showConfirmDelete, setShowConfirmDelete] = useState<boolean>(false);
    const [showEditLinkModal, setShowEditLinkModal] = useState(false);
    const [selectedLink, setSelectedLink] = useState<InstitutionalMemoryMetadata | null>(null);
    const [selectedDocumentUrn, setSelectedDocumentUrn] = useState<string | null>(null);
    const [showAddContextPopover, setShowAddContextPopover] = useState(false);

    const links = useMemo(
        () => entityData?.institutionalMemory?.elements || [],
        [entityData?.institutionalMemory?.elements],
    );
    const { handleDeleteLink } = useLinkUtils(selectedLink);

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
        documents,
        loading: documentsLoading,
        error: documentsError,
        refetch: refetchRelatedDocuments,
    } = useRelatedDocuments(urn || '', {
        count: 100,
    });

    const {
        documentUrnToRemove,
        removedUrns: removedDocumentUrns,
        requestRemove: setDocumentUrnToRemove,
        cancelRemove: cancelRemoveDocument,
        confirmRemove: handleConfirmRemoveDocument,
    } = useRemoveDocumentFromResources({
        entityUrn: urn,
        documents,
        refetch: refetchRelatedDocuments,
        successMessage: t('links.removeDocumentSuccess'),
        errorMessage: t('links.removeDocumentError'),
    });

    // Filter out docs the user just removed so the pill disappears immediately and
    // stays gone while the ES-backed query catches up (see the hook for details).
    const visibleDocuments = useMemo(
        () => (removedDocumentUrns.size > 0 ? documents.filter((d) => !removedDocumentUrns.has(d.urn)) : documents),
        [documents, removedDocumentUrns],
    );

    // Memoize the delete callback to prevent unnecessary re-renders
    const handleDocumentDeleted = useCallback(() => {
        // Wait a moment for delete to complete, then refetch related documents
        setTimeout(() => {
            refetchRelatedDocuments();
        }, 2000);
    }, [refetchRelatedDocuments]);

    const handleAddLink = useCallback(() => {
        setIsAddLinkModalVisible(true);
    }, []);

    const handleAddContext = useCallback(() => {
        setShowAddContextPopover(true);
    }, []);

    const handleDocumentSelected = useCallback((documentUrn: string) => {
        setSelectedDocumentUrn(documentUrn);
        setShowAddContextPopover(false);
        // Don't refetch here - wait until modal closes to avoid duplicate refetches
        // that could cause duplicate links to appear
    }, []);

    const handleDocumentModalClose = useCallback(() => {
        setSelectedDocumentUrn(null);
        // Refetch related documents after modal closes to show any changes made in the modal
        refetchRelatedDocuments();
    }, [refetchRelatedDocuments]);

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

    const handleDelete = () => {
        if (selectedLink) {
            handleDeleteLink().then(() => {
                setSelectedLink(null);
                setShowConfirmDelete(false);
            });
        }
    };

    const handleCancelDelete = () => {
        setShowConfirmDelete(false);
        setSelectedLink(null);
    };

    const handleCloseUpdate = () => {
        setShowEditLinkModal(false);
        setSelectedLink(null);
    };

    const hasLinks = links.length > 0;
    const hasDocuments = supportsRelatedDocuments && visibleDocuments.length > 0 && !documentsError;
    const hasContent = hasRelatedContent(hasLinks, hasDocuments);

    // Docs already in this section render pre-checked in the picker so users can
    // add/remove by toggling. Using `visibleDocuments` means a doc the user just
    // removed shows unchecked immediately, without waiting for the ES refetch.
    const linkedDocumentUrns = useMemo(
        () => (hasDocuments ? visibleDocuments.map((d) => d.urn) : []),
        [hasDocuments, visibleDocuments],
    );

    // Combine and sort items by time (links by created time, documents by lastModified time)
    const sortedItems = useMemo<RelatedItem[]>(
        () => combineAndSortRelatedItems(links, hasDocuments ? visibleDocuments : null),
        [links, hasDocuments, visibleDocuments],
    );

    const itemCount = sortedItems.length;
    const { isExpanded, toggle } = useResourcesCollapseState(itemCount);
    const canToggle = itemCount > 0;

    // Don't show section if there's no content and entity doesn't support related documents
    if (!hasContent && !documentsLoading && !supportsRelatedDocuments) {
        return null;
    }

    return (
        <>
            <SectionHeader data-testid="resources-section-header">
                <HeaderLeft>
                    <SectionTitle weight="bold" color="text" size="sm">
                        {t('links.resourcesTitle')}
                    </SectionTitle>
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
                    {supportsRelatedDocuments && !hideLinksButton && menuItems.length > 0 && (
                        <Popover
                            open={showAddContextPopover}
                            trigger="click"
                            onOpenChange={(visible) => !visible && setShowAddContextPopover(false)}
                            content={
                                urn ? (
                                    <AddContextDocumentPopover
                                        entityUrn={urn}
                                        onDocumentSelected={handleDocumentSelected}
                                        onDocumentsLinked={refetchRelatedDocuments}
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
                                <Tooltip title={t('links.addTooltip')}>
                                    <Button
                                        variant="text"
                                        color="gray"
                                        size="xs"
                                        icon={{ icon: Plus, size: 'lg' }}
                                        style={{ padding: '0 2px' }}
                                        aria-label={t('links.addTooltip')}
                                        data-testid="add-related-button"
                                    />
                                </Tooltip>
                            </Menu>
                        </Popover>
                    )}
                    {canToggle && (
                        <Tooltip title={isExpanded ? t('links.collapseTooltip') : t('links.expandTooltip')}>
                            <Button
                                variant="text"
                                color="gray"
                                size="xs"
                                icon={{ icon: isExpanded ? CaretDown : CaretRight, size: 'lg' }}
                                style={{ padding: '0 2px' }}
                                onClick={toggle}
                                aria-label={isExpanded ? t('links.collapseTooltip') : t('links.expandTooltip')}
                                aria-expanded={isExpanded}
                                data-testid="toggle-resources-button"
                            />
                        </Tooltip>
                    )}
                </HeaderRight>
            </SectionHeader>

            {isExpanded && sortedItems.length > 0 && (
                <ListContainer>
                    {sortedItems.map((item) => {
                        if (item.type === 'link') {
                            return (
                                <ResourceLinkPill
                                    key={`link-${item.data.url}`}
                                    link={item.data}
                                    onEdit={(link) => {
                                        setSelectedLink(link);
                                        setShowEditLinkModal(true);
                                    }}
                                    onDelete={(link) => {
                                        setSelectedLink(link);
                                        setShowConfirmDelete(true);
                                    }}
                                />
                            );
                        }
                        return (
                            <ResourceDocumentPill
                                key={`document-${item.data.urn}`}
                                document={item.data}
                                onClick={setSelectedDocumentUrn}
                                onRemove={setDocumentUrnToRemove}
                                canRemove={canRemoveDocuments}
                            />
                        );
                    })}
                </ListContainer>
            )}

            {!hasContent && !documentsLoading && <EmptyState>{t('links.empty')}</EmptyState>}

            <ConfirmationModal
                isOpen={showConfirmDelete}
                handleClose={handleCancelDelete}
                handleConfirm={handleDelete}
                modalTitle={t('links.deleteConfirmTitle')}
                modalText={t('links.deleteConfirmText')}
                confirmButtonText={ta('delete')}
                isDeleteModal
            />
            <ConfirmationModal
                isOpen={documentUrnToRemove !== null}
                handleClose={cancelRemoveDocument}
                handleConfirm={handleConfirmRemoveDocument}
                modalTitle={t('links.removeDocumentConfirmTitle')}
                modalText={t('links.removeDocumentConfirmText')}
                confirmButtonText={ta('remove')}
                isDeleteModal
            />
            {showEditLinkModal && <EditLinkModal link={selectedLink} onClose={handleCloseUpdate} />}
            {isAddLinkModalVisible && (
                <AddLinkModalUpdated setShowAddLinkModal={(show) => setIsAddLinkModalVisible(show)} />
            )}
            {selectedDocumentUrn && (
                <DocumentModal
                    documentUrn={selectedDocumentUrn}
                    onClose={handleDocumentModalClose}
                    onDocumentDeleted={handleDocumentDeleted}
                />
            )}
        </>
    );
}

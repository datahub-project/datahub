import React, { useCallback, useMemo, useState } from 'react';
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
import {
    RelatedItem,
    combineAndSortRelatedItems,
    createRelatedSectionMenuItems,
    hasRelatedContent,
} from '@app/entityV2/shared/tabs/Documentation/components/relatedSectionUtils';
import DocumentItem from '@app/entityV2/summary/links/DocumentItem';
import LinkItem from '@app/entityV2/summary/links/LinkItem';
import { useLinkPermission } from '@app/entityV2/summary/links/useLinkPermission';
import { ConfirmationModal } from '@app/sharedV2/modals/ConfirmationModal';
import { useIsContextDocumentsEnabled } from '@app/useAppConfig';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { Button, Menu, Popover, Text, Tooltip } from '@src/alchemy-components';
import colors from '@src/alchemy-components/theme/foundations/colors';

import { InstitutionalMemoryMetadata } from '@types';

const SectionHeader = styled.div`
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-top: 16px;
`;

const SectionTitle = styled(Text)`
    font-weight: 700;
    color: ${colors.gray[600]};
    font-size: 12px;
`;

const ListContainer = styled.div`
    display: flex;
    flex-direction: column;
    gap: 8px;
    margin-top: 8px;
`;

const EmptyState = styled.div`
    font-size: 12px;
    color: ${colors.gray[1800]};
    padding: 8px 0;
`;

interface RelatedSectionProps {
    hideLinksButton?: boolean;
}

export default function RelatedSection({ hideLinksButton }: RelatedSectionProps) {
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
    const hasDocuments = supportsRelatedDocuments && documents && documents.length > 0 && !documentsError;
    const hasContent = hasRelatedContent(hasLinks, hasDocuments);

    // Combine and sort items by time (links by created time, documents by lastModified time)
    const sortedItems = useMemo<RelatedItem[]>(
        () => combineAndSortRelatedItems(links, hasDocuments ? documents : null),
        [links, hasDocuments, documents],
    );

    // Don't show section if there's no content and entity doesn't support related documents
    if (!hasContent && !documentsLoading && !supportsRelatedDocuments) {
        return null;
    }

    return (
        <>
            <SectionHeader>
                <SectionTitle weight="bold" color="gray" colorLevel={600} size="sm">
                    Resources
                </SectionTitle>
                {supportsRelatedDocuments && !hideLinksButton && menuItems.length > 0 && (
                    <>
                        <Menu items={menuItems} placement="bottomRight">
                            <Tooltip title="Add related link or context">
                                <Button
                                    variant="text"
                                    color="gray"
                                    size="xs"
                                    icon={{ icon: 'Plus', source: 'phosphor', size: 'lg' }}
                                    style={{ padding: '0 2px' }}
                                    aria-label="Add related link or context"
                                    data-testid="add-related-button"
                                />
                            </Tooltip>
                        </Menu>
                        <Popover
                            open={showAddContextPopover}
                            onOpenChange={(visible) => !visible && setShowAddContextPopover(false)}
                            content={
                                urn ? (
                                    <AddContextDocumentPopover
                                        entityUrn={urn}
                                        onDocumentSelected={handleDocumentSelected}
                                        onClose={() => setShowAddContextPopover(false)}
                                    />
                                ) : null
                            }
                            placement="rightTop"
                            overlayStyle={{ padding: 0 }}
                            overlayInnerStyle={{
                                padding: 0,
                                background: 'transparent',
                                boxShadow: 'none',
                            }}
                        >
                            <span style={{ position: 'absolute', pointerEvents: 'none' }} />
                        </Popover>
                    </>
                )}
            </SectionHeader>

            {sortedItems.length > 0 && (
                <ListContainer>
                    {sortedItems.map((item) => {
                        if (item.type === 'link') {
                            return (
                                <LinkItem
                                    key={`link-${item.data.url}`}
                                    link={item.data}
                                    setSelectedLink={setSelectedLink}
                                    setShowConfirmDelete={setShowConfirmDelete}
                                    setShowEditLinkModal={setShowEditLinkModal}
                                />
                            );
                        }
                        return (
                            <DocumentItem
                                key={`document-${item.data.urn}`}
                                document={item.data}
                                onClick={setSelectedDocumentUrn}
                            />
                        );
                    })}
                </ListContainer>
            )}

            {!hasContent && !documentsLoading && <EmptyState>No related links or context yet</EmptyState>}

            <ConfirmationModal
                isOpen={showConfirmDelete}
                handleClose={handleCancelDelete}
                handleConfirm={handleDelete}
                modalTitle="Confirm Delete"
                modalText="Are you sure you want to delete this link?"
                confirmButtonText="Delete"
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

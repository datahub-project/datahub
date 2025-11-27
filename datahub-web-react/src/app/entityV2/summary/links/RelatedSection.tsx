import React, { useCallback, useMemo, useState } from 'react';
import styled from 'styled-components';

import { useRelatedDocuments } from '@app/document/hooks/useRelatedDocuments';
import { useEntityData } from '@app/entity/shared/EntityContext';
import { EntityCapabilityType } from '@app/entityV2/Entity';
import { DocumentModal } from '@app/entityV2/document/DocumentModal';
import AddLinkModalUpdated from '@app/entityV2/shared/components/links/AddLinkModal';
import { EditLinkModal } from '@app/entityV2/shared/components/links/EditLinkModal';
import { useLinkUtils } from '@app/entityV2/shared/components/links/useLinkUtils';
import DocumentItem from '@app/entityV2/summary/links/DocumentItem';
import LinkItem from '@app/entityV2/summary/links/LinkItem';
import { ConfirmationModal } from '@app/sharedV2/modals/ConfirmationModal';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { Button, Menu, Text, Tooltip } from '@src/alchemy-components';
import { ItemType } from '@src/alchemy-components/components/Menu/types';
import colors from '@src/alchemy-components/theme/foundations/colors';

import { Document, InstitutionalMemoryMetadata } from '@types';

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

type RelatedItem =
    | { type: 'link'; data: InstitutionalMemoryMetadata; sortTime: number }
    | { type: 'document'; data: Document; sortTime: number };

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

    const links = useMemo(
        () => entityData?.institutionalMemory?.elements || [],
        [entityData?.institutionalMemory?.elements],
    );
    const { handleDeleteLink } = useLinkUtils(selectedLink);

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

    const handleAddLink = () => {
        setIsAddLinkModalVisible(true);
    };

    const handleAddContext = () => {
        // Placeholder - do nothing for now
    };

    const menuItems: ItemType[] = [
        {
            type: 'item',
            key: 'add-link',
            title: 'Add link',
            icon: 'LinkSimple',
            onClick: handleAddLink,
        },
        {
            type: 'item',
            key: 'add-context',
            title: 'Add context',
            icon: 'FileText',
            onClick: handleAddContext,
        },
    ];

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
    const hasContent = hasLinks || hasDocuments;

    // Combine and sort items by time (links by created time, documents by lastModified time)
    const sortedItems = useMemo<RelatedItem[]>(() => {
        const items: RelatedItem[] = [];

        // Add links with their created time
        links.forEach((link) => {
            items.push({
                type: 'link',
                data: link,
                sortTime: link.created?.time || 0,
            });
        });

        // Add documents with their lastModified time
        if (hasDocuments && documents) {
            documents.forEach((doc) => {
                items.push({
                    type: 'document',
                    data: doc,
                    sortTime: doc.info?.lastModified?.time || 0,
                });
            });
        }

        // Sort by time descending (most recent first)
        return items.sort((a, b) => b.sortTime - a.sortTime);
    }, [links, hasDocuments, documents]);

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
                {supportsRelatedDocuments && !hideLinksButton && (
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
                    onClose={() => setSelectedDocumentUrn(null)}
                    onDocumentDeleted={handleDocumentDeleted}
                />
            )}
        </>
    );
}

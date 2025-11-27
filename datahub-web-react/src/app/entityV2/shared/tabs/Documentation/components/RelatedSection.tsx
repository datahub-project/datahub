import { List } from 'antd';
import React, { useCallback, useMemo, useState } from 'react';
import styled from 'styled-components';

import { useRelatedDocuments } from '@app/document/hooks/useRelatedDocuments';
import { useEntityData } from '@app/entity/shared/EntityContext';
import { EntityCapabilityType } from '@app/entityV2/Entity';
import { DocumentModal } from '@app/entityV2/document/DocumentModal';
import AddLinkModalUpdated from '@app/entityV2/shared/components/links/AddLinkModal';
import { EditLinkModal } from '@app/entityV2/shared/components/links/EditLinkModal';
import { useLinkUtils } from '@app/entityV2/shared/components/links/useLinkUtils';
import { AddContextDocumentPopover } from '@app/entityV2/shared/tabs/Documentation/components/AddContextDocumentPopover';
import { RelatedDocumentItem } from '@app/entityV2/shared/tabs/Documentation/components/RelatedDocumentItem';
import { RelatedLinkItem } from '@app/entityV2/shared/tabs/Documentation/components/RelatedLinkItem';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { Button, Menu, Popover, Tooltip } from '@src/alchemy-components';
import { ItemType } from '@src/alchemy-components/components/Menu/types';
import colors from '@src/alchemy-components/theme/foundations/colors';

import { Document, InstitutionalMemoryMetadata } from '@types';

const SectionContainer = styled.div`
    margin: 0 16px;
    padding: 0px;
`;

const SectionHeader = styled.div`
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 0px;
`;

const SectionTitle = styled.h3`
    font-size: 14px;
    font-weight: 600;
    color: ${colors.gray[600]};
    margin: 0;
`;

const EmptyState = styled.div`
    font-size: 12px;
    color: ${colors.gray[1800]};
    padding: 8px 0;
`;

type RelatedItem =
    | { type: 'link'; data: InstitutionalMemoryMetadata; sortTime: number }
    | { type: 'document'; data: Document; sortTime: number };

export const RelatedSection: React.FC = () => {
    const { urn, entityData, entityType } = useEntityData();
    const entityRegistry = useEntityRegistry();
    const [isEditFormModalOpened, setIsEditFormModalOpened] = useState<boolean>(false);
    const [editingMetadata, setEditingMetadata] = useState<InstitutionalMemoryMetadata>();
    const [isAddLinkModalVisible, setIsAddLinkModalVisible] = useState(false);
    const [selectedDocumentUrn, setSelectedDocumentUrn] = useState<string | null>(null);
    const [showAddContextPopover, setShowAddContextPopover] = useState(false);

    const links = useMemo(
        () => entityData?.institutionalMemory?.elements || [],
        [entityData?.institutionalMemory?.elements],
    );
    const { handleDeleteLink } = useLinkUtils(editingMetadata);

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

    const onEdit = useCallback((metadata: InstitutionalMemoryMetadata) => {
        setEditingMetadata(metadata);
        setIsEditFormModalOpened(true);
    }, []);

    const onEditFormModalClosed = useCallback(() => {
        setEditingMetadata(undefined);
        setIsEditFormModalOpened(false);
    }, []);

    const handleDocumentClick = useCallback((documentUrn: string) => {
        setSelectedDocumentUrn(documentUrn);
    }, []);

    const handleAddLink = () => {
        setIsAddLinkModalVisible(true);
    };

    // Memoize the delete callback to prevent unnecessary re-renders
    const handleDocumentDeleted = useCallback(() => {
        // Wait a moment for delete to complete, then refetch related documents
        setTimeout(() => {
            refetchRelatedDocuments();
        }, 2000);
    }, [refetchRelatedDocuments]);

    const handleAddContext = () => {
        setShowAddContextPopover(true);
    };

    const handleDocumentSelected = useCallback(
        (documentUrn: string) => {
            setSelectedDocumentUrn(documentUrn);
            setShowAddContextPopover(false);
            // Refetch related documents to show the newly linked document
            setTimeout(() => {
                refetchRelatedDocuments();
            }, 1000);
        },
        [refetchRelatedDocuments],
    );

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

    return (
        <>
            <SectionContainer>
                <SectionHeader>
                    <SectionTitle>Resources</SectionTitle>
                    {supportsRelatedDocuments && (
                        <>
                            <Menu items={menuItems} placement="bottomRight">
                                <Tooltip title="Add related link or context">
                                    <Button
                                        variant="text"
                                        isCircle
                                        icon={{ icon: 'Plus', source: 'phosphor' }}
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
                    <List
                        data-testid="related-list"
                        size="large"
                        dataSource={sortedItems}
                        renderItem={(item) => {
                            if (item.type === 'link') {
                                return <RelatedLinkItem link={item.data} onEdit={onEdit} onDelete={handleDeleteLink} />;
                            }
                            return <RelatedDocumentItem document={item.data} onClick={handleDocumentClick} />;
                        }}
                    />
                )}

                {!hasContent && !documentsLoading && <EmptyState>No related links or context yet</EmptyState>}
            </SectionContainer>

            {isEditFormModalOpened && <EditLinkModal link={editingMetadata} onClose={onEditFormModalClosed} />}
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
};

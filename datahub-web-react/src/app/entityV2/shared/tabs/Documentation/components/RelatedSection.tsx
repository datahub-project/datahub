import { List } from 'antd';
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
import { RelatedDocumentItem } from '@app/entityV2/shared/tabs/Documentation/components/RelatedDocumentItem';
import { RelatedLinkItem } from '@app/entityV2/shared/tabs/Documentation/components/RelatedLinkItem';
import {
    combineAndSortRelatedItems,
    createRelatedSectionMenuItems,
    hasRelatedContent,
} from '@app/entityV2/shared/tabs/Documentation/components/relatedSectionUtils';
import { useLinkPermission } from '@app/entityV2/summary/links/useLinkPermission';
import { useIsContextDocumentsEnabled } from '@app/useAppConfig';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { Button, Menu, Popover, Tooltip } from '@src/alchemy-components';
import colors from '@src/alchemy-components/theme/foundations/colors';

import { InstitutionalMemoryMetadata } from '@types';

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

    const handleAddLink = useCallback(() => {
        setIsAddLinkModalVisible(true);
    }, []);

    // Memoize the delete callback to prevent unnecessary re-renders
    const handleDocumentDeleted = useCallback(() => {
        // Wait a moment for delete to complete, then refetch related documents
        setTimeout(() => {
            refetchRelatedDocuments();
        }, 2000);
    }, [refetchRelatedDocuments]);

    const handleAddContext = useCallback(() => {
        setShowAddContextPopover(true);
    }, []);

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
    const hasDocuments = supportsRelatedDocuments && documents && documents.length > 0 && !documentsError;
    const hasContent = hasRelatedContent(hasLinks, hasDocuments);

    // Combine and sort items by time (links by created time, documents by lastModified time)
    const sortedItems = useMemo(
        () => combineAndSortRelatedItems(links, hasDocuments ? documents : null),
        [links, hasDocuments, documents],
    );

    return (
        <>
            <SectionContainer>
                <SectionHeader>
                    <SectionTitle>Resources</SectionTitle>
                    {supportsRelatedDocuments && menuItems.length > 0 && (
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

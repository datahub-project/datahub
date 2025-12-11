import { Button, Tooltip, colors } from '@components';
import React, { useState } from 'react';
import { useHistory } from 'react-router-dom';
import styled from 'styled-components';

import { DocumentTreeNode } from '@app/document/DocumentTreeContext';
import { useDocumentPermissions } from '@app/document/hooks/useDocumentPermissions';
import { useEntityData } from '@app/entity/shared/EntityContext';
import { DocumentChangeHistoryDrawer } from '@app/entityV2/document/changeHistory/DocumentChangeHistoryDrawer';
import { EditableContent } from '@app/entityV2/document/summary/EditableContent';
import { EditableTitle } from '@app/entityV2/document/summary/EditableTitle';
import PropertiesHeader from '@app/entityV2/summary/properties/PropertiesHeader';
import { DocumentActionsMenu } from '@app/homeV2/layout/sidebar/documents/DocumentActionsMenu';
import { useModalContext } from '@app/sharedV2/modals/ModalContext';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { Document, EntityType } from '@types';

const SummaryWrapper = styled.div`
    padding: 40px 20%;
    display: flex;
    flex-direction: column;
    gap: 16px;
`;

const HeaderRow = styled.div`
    display: flex;
    align-items: flex-start;
    gap: 16px;
    width: 100%;
`;

const TitleSection = styled.div`
    flex: 1;
    min-width: 0; /* Critical: allow flex item to shrink below its content size */
`;

const TopRightButtonsContainer = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
    flex-shrink: 0; /* Buttons take precedence - don't shrink */
`;

const TopRightButton = styled(Button)`
    background: transparent;
    border: none;
    cursor: pointer;
    padding: 8px;
    border-radius: 6px;
    display: flex;
    align-items: center;
    justify-content: center;
    color: ${colors.gray[400]};

    &:hover {
        background-color: ${colors.gray[100]};
    }
`;

const ActionsMenuWrapper = styled.div`
    display: flex;
    align-items: center;

    /* Style the menu button to match other top right buttons */
    button {
        background: transparent;
        border: none;
        cursor: pointer;
        padding: 8px;
        border-radius: 6px;
        display: flex;
        align-items: center;
        justify-content: center;
        color: ${colors.gray[400]};
        min-width: auto;
        width: auto;
        height: auto;

        &:hover {
            background-color: ${colors.gray[100]};
        }
    }
`;

const Breadcrumb = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
    font-size: 14px;
    color: ${colors.gray[1700]};
    margin-bottom: 0px;
`;

const BreadcrumbLink = styled.a`
    color: ${colors.gray[1700]};
    text-decoration: none;
    cursor: pointer;

    &:hover {
        text-decoration: underline;
    }
`;

const BreadcrumbSeparator = styled.span`
    color: ${colors.gray[1700]};
    margin: 0 4px;
`;

interface DocumentSummaryTabProps {
    onDelete?: (deletedNode: DocumentTreeNode | null) => void;
    onMove?: (documentUrn: string) => void;
}

export const DocumentSummaryTab: React.FC<DocumentSummaryTabProps> = ({ onDelete, onMove }) => {
    const { urn, entityData } = useEntityData();
    const document = entityData as Document;
    const history = useHistory();
    const entityRegistry = useEntityRegistry();
    const { isInsideModal } = useModalContext();
    const [isHistoryDrawerOpen, setIsHistoryDrawerOpen] = useState(false);
    const { canDelete, canMove } = useDocumentPermissions(urn);

    const documentContent = document?.info?.contents?.text || '';

    // Get parent documents hierarchy (ordered: direct parent, parent's parent, ...)
    const parentDocuments = document?.parentDocuments?.documents || [];
    // Get the direct parent URN (first in the array)
    const currentParentUrn = parentDocuments.length > 0 ? parentDocuments[0].urn : null;

    const handleParentClick = (parentUrn: string) => {
        history.push(entityRegistry.getEntityUrl(EntityType.Document, parentUrn));
    };

    const handleGoToDocument = () => {
        const url = entityRegistry.getEntityUrl(EntityType.Document, urn);
        history.push(url);
    };

    return (
        <>
            <SummaryWrapper key={urn}>
                {/* Header row with title and buttons - uses flexbox for natural wrapping */}
                <HeaderRow>
                    <TitleSection>
                        {/* Parent documents breadcrumb - show full hierarchy */}
                        {parentDocuments.length > 0 && (
                            <Breadcrumb>
                                {[...parentDocuments].reverse().map((parent, index) => (
                                    <React.Fragment key={parent.urn}>
                                        <BreadcrumbLink onClick={() => handleParentClick(parent.urn)}>
                                            {parent.info?.title || 'Untitled'}
                                        </BreadcrumbLink>
                                        {index < parentDocuments.length - 1 && (
                                            <BreadcrumbSeparator>/</BreadcrumbSeparator>
                                        )}
                                    </React.Fragment>
                                ))}
                            </Breadcrumb>
                        )}

                        {/* Simple Notion-style title input - click to edit */}
                        <EditableTitle documentUrn={urn} initialTitle={document?.info?.title || ''} />
                    </TitleSection>

                    {/* Top right buttons - History and Expand (when in modal) */}
                    <TopRightButtonsContainer>
                        <Tooltip title="View change history">
                            <TopRightButton
                                variant="text"
                                onClick={() => setIsHistoryDrawerOpen(true)}
                                aria-label="View change history"
                                icon={{ icon: 'Clock', source: 'phosphor', size: '2xl' }}
                            />
                        </Tooltip>
                        {isInsideModal && (
                            <Tooltip title="Go to document profile">
                                <TopRightButton
                                    variant="text"
                                    onClick={handleGoToDocument}
                                    data-testid="expand-go-to-document-button"
                                    icon={{ icon: 'ArrowSquareOut', source: 'phosphor', size: '2xl' }}
                                />
                            </Tooltip>
                        )}
                        <ActionsMenuWrapper>
                            <DocumentActionsMenu
                                documentUrn={urn}
                                currentParentUrn={currentParentUrn}
                                canDelete={canDelete}
                                canMove={canMove}
                                onDelete={onDelete}
                                shouldNavigateOnDelete={!isInsideModal}
                                onMove={onMove}
                            />
                        </ActionsMenuWrapper>
                    </TopRightButtonsContainer>
                </HeaderRow>

                {/* Properties list - reuses SummaryTab component */}
                <PropertiesHeader />

                {/* Document Contents */}
                <EditableContent
                    documentUrn={urn}
                    initialContent={documentContent}
                    relatedAssets={document?.info?.relatedAssets || undefined}
                    relatedDocuments={document?.info?.relatedDocuments || undefined}
                />
            </SummaryWrapper>

            {/* Change History Drawer */}
            <DocumentChangeHistoryDrawer
                urn={urn}
                open={isHistoryDrawerOpen}
                onClose={() => setIsHistoryDrawerOpen(false)}
            />
        </>
    );
};

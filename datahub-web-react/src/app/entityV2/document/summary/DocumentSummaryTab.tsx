import { Button, Tooltip, colors } from '@components';
import React, { useState } from 'react';
import { useHistory } from 'react-router-dom';
import styled from 'styled-components';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { DocumentChangeHistoryDrawer } from '@app/entityV2/document/changeHistory/DocumentChangeHistoryDrawer';
import { EditableContent } from '@app/entityV2/document/summary/EditableContent';
import { EditableTitle } from '@app/entityV2/document/summary/EditableTitle';
import PropertiesHeader from '@app/entityV2/summary/properties/PropertiesHeader';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { Document, EntityType } from '@types';

const SummaryWrapper = styled.div`
    padding: 40px 20%;
    display: flex;
    flex-direction: column;
    gap: 16px;
    position: relative;
`;

const HistoryIconButton = styled(Button)`
    position: absolute;
    top: 40px;
    right: 20%;
    background: transparent;
    border: none;
    cursor: pointer;
    padding: 8px;
    border-radius: 6px;
    display: flex;
    align-items: center;
    justify-content: center;
`;

const Breadcrumb = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
    font-size: 14px;
    color: #8c8c8c;
    margin-bottom: 0px;
`;

const BreadcrumbLink = styled.a`
    color: ${colors.gray[500]};
    text-decoration: none;
    cursor: pointer;

    &:hover {
        text-decoration: underline;
    }
`;

export const DocumentSummaryTab = () => {
    const { urn, entityData } = useEntityData();
    const document = entityData as Document;
    const history = useHistory();
    const entityRegistry = useEntityRegistry();
    const [isHistoryDrawerOpen, setIsHistoryDrawerOpen] = useState(false);

    const parentDocument = document?.info?.parentDocument?.document;
    const parentTitle = parentDocument?.info?.title || 'Parent Document';
    const parentUrn = parentDocument?.urn;
    const documentContent = document?.info?.contents?.text || '';

    const handleParentClick = () => {
        if (parentUrn) {
            history.push(entityRegistry.getEntityUrl(EntityType.Document, parentUrn));
        }
    };

    return (
        <>
            <SummaryWrapper key={urn}>
                {/* History icon button - top right */}
                <Tooltip title="View change history">
                    <HistoryIconButton
                        variant="text"
                        onClick={() => setIsHistoryDrawerOpen(true)}
                        aria-label="View change history"
                        icon={{ icon: 'Clock', source: 'phosphor', size: 'lg', color: 'gray' }}
                    />
                </Tooltip>

                {/* Parent document breadcrumb */}
                {parentDocument && (
                    <Breadcrumb>
                        <BreadcrumbLink onClick={handleParentClick}>{parentTitle}</BreadcrumbLink>
                    </Breadcrumb>
                )}

                {/* Simple Notion-style title input - click to edit */}
                <EditableTitle documentUrn={urn} initialTitle={document?.info?.title || ''} />

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

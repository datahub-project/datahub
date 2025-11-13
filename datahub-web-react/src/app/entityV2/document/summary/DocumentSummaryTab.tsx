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
    color: ${colors.gray[400]};
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

export const DocumentSummaryTab = () => {
    const { urn, entityData } = useEntityData();
    const document = entityData as Document;
    const history = useHistory();
    const entityRegistry = useEntityRegistry();
    const [isHistoryDrawerOpen, setIsHistoryDrawerOpen] = useState(false);

    const documentContent = document?.info?.contents?.text || '';

    // Get parent documents hierarchy (ordered: direct parent, parent's parent, ...)
    const parentDocuments = document?.parentDocuments?.documents || [];

    const handleParentClick = (parentUrn: string) => {
        history.push(entityRegistry.getEntityUrl(EntityType.Document, parentUrn));
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
                        icon={{ icon: 'Clock', source: 'phosphor', size: '2xl' }}
                    />
                </Tooltip>

                {/* Parent documents breadcrumb - show full hierarchy */}
                {parentDocuments.length > 0 && (
                    <Breadcrumb>
                        {[...parentDocuments].reverse().map((parent, index) => (
                            <React.Fragment key={parent.urn}>
                                <BreadcrumbLink onClick={() => handleParentClick(parent.urn)}>
                                    {parent.info?.title || 'Untitled'}
                                </BreadcrumbLink>
                                {index < parentDocuments.length - 1 && <BreadcrumbSeparator>/</BreadcrumbSeparator>}
                            </React.Fragment>
                        ))}
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

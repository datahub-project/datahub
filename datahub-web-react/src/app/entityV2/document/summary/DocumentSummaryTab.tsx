import { colors } from '@components';
import React from 'react';
import { useHistory } from 'react-router-dom';
import styled from 'styled-components';

import { useEntityData } from '@app/entity/shared/EntityContext';
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

    const parentDocument = document?.info?.parentDocument?.document;
    const parentTitle = parentDocument?.info?.title || 'Parent Document';
    const parentUrn = parentDocument?.urn;

    const handleParentClick = () => {
        if (parentUrn) {
            history.push(entityRegistry.getEntityUrl(EntityType.Document, parentUrn));
        }
    };

    return (
        <SummaryWrapper key={urn}>
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
                initialContent={document?.info?.contents?.text || ''}
                relatedAssets={document?.info?.relatedAssets || undefined}
                relatedDocuments={document?.info?.relatedDocuments || undefined}
            />
        </SummaryWrapper>
    );
};

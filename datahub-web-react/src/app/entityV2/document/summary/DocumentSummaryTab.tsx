import React from 'react';
import styled from 'styled-components';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { DocumentInfoSection } from '@app/entityV2/document/summary/DocumentInfoSection';
import { EditableContent } from '@app/entityV2/document/summary/EditableContent';
import { EditableTitle } from '@app/entityV2/document/summary/EditableTitle';
import PropertiesHeader from '@app/entityV2/summary/properties/PropertiesHeader';

import { Document } from '@types';

const SummaryWrapper = styled.div`
    padding: 16px 20px;
    display: flex;
    flex-direction: column;
    gap: 16px;
`;

export const DocumentSummaryTab = () => {
    const { urn, entityData } = useEntityData();
    const document = entityData as Document;

    return (
        <SummaryWrapper key={urn}>
            {/* Title comes first, above properties */}
            <EditableTitle documentUrn={urn} initialTitle={document?.info?.title || 'Untitled Document'} />

            <PropertiesHeader />
            <DocumentInfoSection />

            {/* Content comes after properties */}
            <EditableContent
                documentUrn={urn}
                initialContent={document?.info?.contents?.text || ''}
                relatedAssets={document?.info?.relatedAssets || undefined}
                relatedDocuments={document?.info?.relatedDocuments || undefined}
            />
        </SummaryWrapper>
    );
};

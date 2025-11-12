import React from 'react';
import styled from 'styled-components';

import { SectionContainer } from '@app/entityV2/shared/summary/HeaderComponents';
import { EntityLink } from '@app/homeV2/reference/sections/EntityLink';
import { useEntityRegistry } from '@app/useEntityRegistry';
import colors from '@src/alchemy-components/theme/foundations/colors';

import { DocumentRelatedDocument, EntityType } from '@types';

const SectionHeader = styled.h4`
    font-size: 16px;
    font-weight: 600;
    margin: 0 0 8px 0;
    color: ${colors.gray[1700]};
`;

const List = styled.div`
    display: flex;
    flex-direction: column;
    gap: 4px;
`;

interface RelatedDocumentsSectionProps {
    relatedDocuments?: DocumentRelatedDocument[];
}

export const RelatedDocumentsSection: React.FC<RelatedDocumentsSectionProps> = ({ relatedDocuments }) => {
    const entityRegistry = useEntityRegistry();

    if (!relatedDocuments || relatedDocuments.length === 0) {
        return null;
    }

    return (
        <SectionContainer>
            <SectionHeader>Related Documents</SectionHeader>
            <List>
                {relatedDocuments.map((relatedDoc) => {
                    const { document } = relatedDoc;
                    const genericProperties = entityRegistry.getGenericEntityProperties(EntityType.Document, document);

                    return (
                        <EntityLink
                            key={document.urn}
                            entity={genericProperties}
                            showHealthIcon={false}
                            showDeprecatedIcon={false}
                        />
                    );
                })}
            </List>
        </SectionContainer>
    );
};

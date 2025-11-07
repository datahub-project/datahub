import { FileText } from '@phosphor-icons/react';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { SectionContainer } from '@app/entityV2/shared/summary/HeaderComponents';
import { useEntityRegistry } from '@app/useEntityRegistry';
import colors from '@src/alchemy-components/theme/foundations/colors';

import { DocumentRelatedDocument, EntityType } from '@types';

const SectionHeader = styled.h4`
    font-size: 16px;
    font-weight: 600;
    margin: 0;
    color: ${colors.gray[1700]};
`;

const RelatedList = styled.div`
    display: flex;
    flex-direction: column;
    gap: 8px;
`;

const RelatedItem = styled(Link)`
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 8px;
    border-radius: 6px;
    background-color: ${colors.gray[0]};
    border: 1px solid ${colors.gray[100]};
    text-decoration: none;
    color: inherit;
    transition: all 0.2s ease;

    &:hover {
        background-color: ${colors.violet[0]};
        border-color: ${colors.violet[100]};
    }
`;

const IconWrapper = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    color: ${colors.violet[600]};
`;

const ItemContent = styled.div`
    display: flex;
    flex-direction: column;
`;

const ItemTitle = styled.div`
    font-size: 14px;
    font-weight: 500;
    color: ${colors.gray[1700]};
`;

const ItemType = styled.div`
    font-size: 12px;
    color: ${colors.gray[1800]};
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
            <RelatedList>
                {relatedDocuments.map((relatedDoc) => {
                    const { document } = relatedDoc;
                    const url = entityRegistry.getEntityUrl(EntityType.Document, document.urn);
                    const displayName = document.info?.title || document.urn;

                    return (
                        <RelatedItem key={document.urn} to={url}>
                            <IconWrapper>
                                <FileText size={16} weight="duotone" />
                            </IconWrapper>
                            <ItemContent>
                                <ItemTitle>{displayName}</ItemTitle>
                                <ItemType>Document</ItemType>
                            </ItemContent>
                        </RelatedItem>
                    );
                })}
            </RelatedList>
        </SectionContainer>
    );
};

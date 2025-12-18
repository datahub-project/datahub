import React from 'react';
import { useHistory } from 'react-router-dom';
import styled from 'styled-components';

import { useRelatedDocuments } from '@app/document/hooks/useRelatedDocuments';
import { useEntityData } from '@app/entity/shared/EntityContext';
import { EntityCapabilityType } from '@app/entityV2/Entity';
import { useEntityRegistry } from '@app/useEntityRegistry';
import colors from '@src/alchemy-components/theme/foundations/colors';

import { EntityType } from '@types';

const SectionContainer = styled.div`
    margin: 0 32px;
    padding: 24px 0;
    border-top: 1px solid ${colors.gray[200]};
`;

const SectionTitle = styled.h3`
    font-size: 16px;
    font-weight: 600;
    color: ${colors.gray[1700]};
    margin: 0 0 16px 0;
`;

const DocumentsList = styled.div`
    display: flex;
    flex-direction: column;
    gap: 8px;
`;

const DocumentItem = styled.div`
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 8px 12px;
    border-radius: 4px;
    cursor: pointer;
    transition: background-color 0.2s ease;

    &:hover {
        background-color: ${colors.gray[100]};
    }
`;

const DocumentTitle = styled.span`
    font-size: 14px;
    color: ${colors.gray[1700]};
    font-weight: 500;
`;

const DocumentMetadata = styled.div`
    display: flex;
    align-items: center;
    gap: 12px;
    font-size: 12px;
    color: ${colors.gray[600]};
`;

const EmptyState = styled.div`
    font-size: 14px;
    color: ${colors.gray[600]};
    font-style: italic;
    padding: 8px 0;
`;

const formatDate = (timestamp: number | null | undefined): string => {
    if (!timestamp) {
        return '';
    }
    const date = new Date(timestamp);
    const now = new Date();
    const diffMs = now.getTime() - date.getTime();
    const diffDays = Math.floor(diffMs / (1000 * 60 * 60 * 24));

    if (diffDays === 0) {
        return 'Today';
    }
    if (diffDays === 1) {
        return 'Yesterday';
    }
    if (diffDays < 7) {
        return `${diffDays} days ago`;
    }
    if (diffDays < 30) {
        const weeks = Math.floor(diffDays / 7);
        return `${weeks} ${weeks === 1 ? 'week' : 'weeks'} ago`;
    }
    if (diffDays < 365) {
        const months = Math.floor(diffDays / 30);
        return `${months} ${months === 1 ? 'month' : 'months'} ago`;
    }
    const years = Math.floor(diffDays / 365);
    return `${years} ${years === 1 ? 'year' : 'years'} ago`;
};

export const RelatedContextDocuments: React.FC = () => {
    const { urn, entityType } = useEntityData();
    const entityRegistry = useEntityRegistry();
    const history = useHistory();
    const { documents, loading, error } = useRelatedDocuments(urn || '', {
        count: 100, // Limit to 100 most recently updated
    });

    // Only show for entity types that support the RELATED_DOCUMENTS capability
    if (!urn || !entityType) {
        return null;
    }

    const supportedCapabilities = entityRegistry.getSupportedEntityCapabilities(entityType);
    if (!supportedCapabilities.has(EntityCapabilityType.RELATED_DOCUMENTS)) {
        return null;
    }

    if (loading) {
        return (
            <SectionContainer>
                <SectionTitle>Related Context</SectionTitle>
                <EmptyState>Loading...</EmptyState>
            </SectionContainer>
        );
    }

    if (error) {
        return null; // Silently fail - don't show error state
    }

    if (!documents || documents.length === 0) {
        return null; // Don't show section if no documents
    }

    const handleDocumentClick = (documentUrn: string) => {
        const url = entityRegistry.getEntityUrl(EntityType.Document, documentUrn);
        history.push(url);
    };

    return (
        <SectionContainer>
            <SectionTitle>Related Context</SectionTitle>
            <DocumentsList>
                {documents.map((doc) => (
                    <DocumentItem
                        key={doc.urn}
                        onClick={() => handleDocumentClick(doc.urn)}
                        data-testid={`related-context-document-${doc.urn.split(':').pop()}`}
                    >
                        <DocumentTitle>{doc.info?.title || 'Untitled Document'}</DocumentTitle>
                        <DocumentMetadata>
                            {doc.info?.lastModified?.time && <span>{formatDate(doc.info.lastModified.time)}</span>}
                        </DocumentMetadata>
                    </DocumentItem>
                ))}
            </DocumentsList>
        </SectionContainer>
    );
};

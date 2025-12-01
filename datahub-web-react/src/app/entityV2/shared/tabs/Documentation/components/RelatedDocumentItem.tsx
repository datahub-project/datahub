import { FileText } from '@phosphor-icons/react';
import { List } from 'antd';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { formatDateString } from '@app/entityV2/shared/containers/profile/utils';
import { getActorDisplayName, isActor } from '@app/entityV2/shared/utils/actorUtils';
import { useEntityRegistry } from '@app/useEntityRegistry';
import colors from '@src/alchemy-components/theme/foundations/colors';

import { Document } from '@types';

const DocumentListItem = styled(List.Item)`
    border-radius: 5px;
    cursor: pointer;
    transition: background-color 0.2s ease;

    &:hover {
        background-color: ${colors.gray[100]};
    }
`;

const MetaContainer = styled.div`
    display: flex;
    gap: 16px;
    width: 100%;
    min-width: 0;
`;

const IconContainer = styled.div`
    display: flex;
    align-items: center;
    flex-shrink: 0;
    margin-top: 2px; /* Slight offset to visually center between title and description */
`;

const ContentContainer = styled.div`
    flex: 1;
    min-width: 0;
    display: flex;
    flex-direction: column;
    gap: 4px;
`;

const TitleLink = styled.a`
    font-size: 14px;
    font-weight: 700;
    line-height: 16px;
    text-decoration: none;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    display: block;
`;

const Description = styled.div`
    font-size: 12px;
    color: ${colors.gray[1800]};
    line-height: 20px;
`;

interface RelatedDocumentItemProps {
    document: Document;
    onClick: (documentUrn: string) => void;
}

export const RelatedDocumentItem: React.FC<RelatedDocumentItemProps> = ({ document, onClick }) => {
    const entityRegistry = useEntityRegistry();
    const lastModified = document.info?.lastModified;
    const actor = lastModified?.actor;

    const handleClick = (e: React.MouseEvent) => {
        e.preventDefault();
        onClick(document.urn);
    };

    return (
        <DocumentListItem
            onClick={handleClick}
            data-testid={`related-context-document-${document.urn.split(':').pop()}`}
        >
            <MetaContainer>
                <IconContainer>
                    <FileText size={16} weight="duotone" color={colors.primary[500]} />
                </IconContainer>
                <ContentContainer>
                    <TitleLink href="#" onClick={handleClick}>
                        {document.info?.title || 'Untitled Document'}
                    </TitleLink>
                    {lastModified?.time && (
                        <Description>
                            Edited {formatDateString(lastModified.time)}
                            {actor && (
                                <>
                                    {' by '}
                                    {(() => {
                                        // New type where actor is a full Entity (CorpUser)
                                        const actorEntity = actor;
                                        return isActor(actorEntity) ? (
                                            <Link
                                                to={`${entityRegistry.getEntityUrl(actorEntity.type, actorEntity.urn)}`}
                                                onClick={(e) => e.stopPropagation()}
                                            >
                                                {getActorDisplayName(actorEntity) ||
                                                    entityRegistry.getDisplayName(actorEntity.type, actorEntity)}
                                            </Link>
                                        ) : null;
                                    })()}
                                </>
                            )}
                        </Description>
                    )}
                </ContentContainer>
            </MetaContainer>
        </DocumentListItem>
    );
};

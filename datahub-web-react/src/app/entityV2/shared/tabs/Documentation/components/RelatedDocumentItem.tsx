import { FileText } from '@phosphor-icons/react/dist/csr/FileText';
import { List } from 'antd';
import React from 'react';
import { Trans, useTranslation } from 'react-i18next';
import { Link } from 'react-router-dom';
import styled, { useTheme } from 'styled-components';

import { formatDateString } from '@app/entityV2/shared/containers/profile/utils';
import { getActorDisplayName, isActor } from '@app/entityV2/shared/utils/actorUtils';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { Document } from '@types';

const DocumentListItem = styled(List.Item)`
    border-radius: 5px;
    cursor: pointer;
    transition: background-color 0.2s ease;

    &:hover {
        background-color: ${(props) => props.theme.colors.bgSurface};
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
    color: ${(props) => props.theme.colors.textTertiary};
    line-height: 20px;
`;

interface RelatedDocumentItemProps {
    document: Document;
    onClick: (documentUrn: string) => void;
}

export const RelatedDocumentItem: React.FC<RelatedDocumentItemProps> = ({ document, onClick }) => {
    const theme = useTheme();
    const entityRegistry = useEntityRegistry();
    const { t } = useTranslation('entity.profile.documentation');
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
                    <FileText size={16} weight="duotone" color={theme.colors.iconBrand} />
                </IconContainer>
                <ContentContainer>
                    <TitleLink href="#" onClick={handleClick}>
                        {document.info?.title || t('untitledDocument')}
                    </TitleLink>
                    {lastModified?.time && (
                        <Description>
                            {actor && isActor(actor) ? (
                                <Trans
                                    i18nKey="editedByUser"
                                    ns="entity.profile.documentation"
                                    values={{
                                        date: formatDateString(lastModified.time),
                                        name:
                                            getActorDisplayName(actor) ||
                                            entityRegistry.getDisplayName(actor.type, actor),
                                    }}
                                    components={{
                                        actorLink: (
                                            <Link
                                                to={`${entityRegistry.getEntityUrl(actor.type, actor.urn)}`}
                                                onClick={(e) => e.stopPropagation()}
                                            />
                                        ),
                                    }}
                                />
                            ) : (
                                formatDateString(lastModified.time)
                            )}
                        </Description>
                    )}
                </ContentContainer>
            </MetaContainer>
        </DocumentListItem>
    );
};

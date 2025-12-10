import { List } from 'antd';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { LinkIcon } from '@app/entityV2/shared/components/links/LinkIcon';
import { formatDateString } from '@app/entityV2/shared/containers/profile/utils';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { Button } from '@src/alchemy-components';
import colors from '@src/alchemy-components/theme/foundations/colors';

import { InstitutionalMemoryMetadata } from '@types';

const LinkButtonsContainer = styled.div`
    display: flex;
    flex-direction: row;
    gap: 8px;
`;

const StyledButton = styled(Button)`
    opacity: 0;
    transition: opacity 0.2s ease;
`;

const LinkListItem = styled(List.Item)`
    border-radius: 5px;
    cursor: pointer;
    transition: background-color 0.2s ease;

    &:hover {
        background-color: ${colors.gray[100]};
        ${LinkButtonsContainer} {
            ${StyledButton} {
                opacity: 1;
            }
        }
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

interface RelatedLinkItemProps {
    link: InstitutionalMemoryMetadata;
    onEdit: (link: InstitutionalMemoryMetadata) => void;
    onDelete: (link: InstitutionalMemoryMetadata) => void;
}

export const RelatedLinkItem: React.FC<RelatedLinkItemProps> = ({ link, onEdit, onDelete }) => {
    const entityRegistry = useEntityRegistry();

    return (
        <LinkListItem
            extra={
                <LinkButtonsContainer>
                    <StyledButton
                        variant="text"
                        isCircle
                        icon={{ icon: 'Pencil', source: 'phosphor', size: 'md', color: 'gray', colorLevel: 500 }}
                        onClick={() => onEdit(link)}
                        data-testid="edit-link-button"
                    />
                    <StyledButton
                        variant="text"
                        isCircle
                        icon={{ icon: 'Trash', source: 'phosphor', size: 'md', color: 'red', colorLevel: 500 }}
                        onClick={() => onDelete(link)}
                        data-testid="remove-link-button"
                    />
                </LinkButtonsContainer>
            }
        >
            <MetaContainer>
                <IconContainer>
                    <LinkIcon url={link.url} />
                </IconContainer>
                <ContentContainer>
                    <TitleLink href={link.url} target="_blank" rel="noreferrer">
                        {link.description || link.label}
                    </TitleLink>
                    <Description>
                        Added {formatDateString(link.created.time)} by{' '}
                        <Link to={`${entityRegistry.getEntityUrl(link.actor.type, link.actor.urn)}`}>
                            {entityRegistry.getDisplayName(link.actor.type, link.actor)}
                        </Link>
                    </Description>
                </ContentContainer>
            </MetaContainer>
        </LinkListItem>
    );
};

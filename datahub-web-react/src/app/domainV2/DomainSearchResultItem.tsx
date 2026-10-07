// Create a new component called SearchResultItem.js
import React from 'react';
import Highlight from 'react-highlighter';
import { useTranslation } from 'react-i18next';
import { Link } from 'react-router-dom';
import styled from 'styled-components/macro';

import { getParentDomains } from '@app/domainV2/utils';
import { IconStyleType } from '@app/entity/Entity';
import EntityRegistry from '@app/entity/EntityRegistry';
import { DomainColoredIcon } from '@app/entityV2/shared/links/DomainColoredIcon';
import ParentEntities from '@app/search/filters/ParentEntities';

import { Domain, Entity, EntityType } from '@types';

type Props = {
    entity: Entity;
    entityRegistry: EntityRegistry;
    query: string;
    /** Display name of the owner the query matched, when the domain was found via its owner. */
    matchedOwnerName?: string;
    onResultClick: () => void;
};

const SearchResult = styled(Link)`
    color: ${(props) => props.theme.colors.text};
    display: flex;
    align-items: center;
    gap: 8px;
    height: 100%;
    padding: 6px 8px;
    overflow: hidden;
    width: 100%;
    &:hover {
        background-color: ${(props) => props.theme.colors.bgSurface};
        color: ${(props) => props.theme.colors.text};
    }
`;

const ContentWrapper = styled.div`
    display: flex;
    flex-direction: column;
    flex-shrink: 1;
    align-self: flex-start;
    overflow: hidden;
`;

const IconWrapper = styled.span``;

const MatchReason = styled.div`
    font-size: 12px;
    color: ${(props) => props.theme.colors.textSecondary};
`;

function DomainSearchResultItem({ entity, entityRegistry, query, matchedOwnerName, onResultClick }: Props) {
    const { t } = useTranslation('governance.domain');
    const highlightMatchStyle = {
        fontWeight: 'bold',
        background: 'none',
        padding: 0,
    };
    return (
        <SearchResult to={entityRegistry.getEntityUrl(entity.type, entity.urn)} onClick={onResultClick}>
            <IconWrapper>
                {entity.type === EntityType.Domain ? (
                    <DomainColoredIcon size={24} fontSize={12} domain={entity as Domain} />
                ) : (
                    entityRegistry.getIcon(entity.type, 12, IconStyleType.ACCENT)
                )}
            </IconWrapper>
            <ContentWrapper>
                <Highlight matchStyle={highlightMatchStyle} search={query}>
                    {entityRegistry.getDisplayName(entity.type, entity)}
                </Highlight>
                <ParentEntities hideIcon parentEntities={getParentDomains(entity, entityRegistry)} />
                {matchedOwnerName && (
                    <MatchReason>
                        <Highlight matchStyle={highlightMatchStyle} search={query}>
                            {t('search.ownedBy', { name: matchedOwnerName })}
                        </Highlight>
                    </MatchReason>
                )}
            </ContentWrapper>
        </SearchResult>
    );
}

export default DomainSearchResultItem;

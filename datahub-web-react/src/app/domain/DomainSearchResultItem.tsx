// Create a new component called SearchResultItem.js
import React from 'react';
import { Link } from 'react-router-dom';
import Highlight from 'react-highlighter';
import styled from 'styled-components/macro';
import { Entity, EntityType } from '../../types.generated';
import { IconStyleType } from '../entity/Entity';
import { ANTD_GRAY } from '../entity/shared/constants';
import DomainIcon from './DomainIcon';
import ParentEntities from '../search/filters/ParentEntities';
import { getParentDomains } from './utils';
import EntityRegistry from '../entity/EntityRegistry';

type Props = {
    entity: Entity;
    entityRegistry: EntityRegistry;
    query: string;
    onResultClick: () => void;
};

const SearchResult = styled(Link)`
    color: #262626;
    display: flex;
    align-items: center;
    gap: 8px;
    height: 100%;
    padding: 6px 8px;
    width: 100%;
    &:hover {
        background-color: ${ANTD_GRAY[3]};
        color: #262626;
    }
`;

const IconWrapper = styled.span``;

const highlightMatchStyle = {
    fontWeight: 'bold',
    background: 'none',
    padding: 0,
};

function DomainSearchResultItem({ entity, entityRegistry, query, onResultClick }: Props) {
    return (
        <SearchResult to={entityRegistry.getEntityUrl(entity.type, entity.urn)} onClick={onResultClick}>
            <IconWrapper>
                {entity.type === EntityType.Domain ? (
                    <DomainIcon
                        style={{
                            fontSize: 16,
                            color: '#BFBFBF',
                        }}
                    />
                ) : (
                    entityRegistry.getIcon(entity.type, 12, IconStyleType.ACCENT)
                )}
            </IconWrapper>
            <div>
                <ParentEntities parentEntities={getParentDomains(entity, entityRegistry)} />
                <Highlight matchStyle={highlightMatchStyle} search={query}>
                    {entityRegistry.getDisplayName(entity.type, entity)}
                </Highlight>
            </div>
        </SearchResult>
    );
}

export default DomainSearchResultItem;

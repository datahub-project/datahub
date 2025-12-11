/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
// Create a new component called SearchResultItem.js
import React from 'react';
import Highlight from 'react-highlighter';
import { Link } from 'react-router-dom';
import styled from 'styled-components/macro';

import DomainIcon from '@app/domain/DomainIcon';
import { getParentDomains } from '@app/domain/utils';
import { IconStyleType } from '@app/entity/Entity';
import EntityRegistry from '@app/entity/EntityRegistry';
import { ANTD_GRAY } from '@app/entity/shared/constants';
import ParentEntities from '@app/search/filters/ParentEntities';

import { Entity, EntityType } from '@types';

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

import { LoadingOutlined } from '@ant-design/icons';
import React from 'react';
import styled from 'styled-components';
import { Entity } from '../../types.generated';
import { getParentDomains } from '../domain/utils';
import EntityRegistry from '../entity/EntityRegistry';
import { ANTD_GRAY } from '../entityV2/shared/constants';
import ParentEntities from '../search/filters/ParentEntities';

const LoadingWrapper = styled.div`
    padding: 8px;
    display: flex;
    justify-content: center;

    svg {
        height: 15px;
        width: 15px;
        color: ${ANTD_GRAY[8]};
    }
`;

interface AntOption {
    label: JSX.Element;
    value: string;
}

export default function domainAutocompleteOptions(
    entities: Entity[],
    loading: boolean,
    entityRegistry: EntityRegistry,
): AntOption[] {
    if (loading) {
        return [
            {
                label: (
                    <LoadingWrapper>
                        <LoadingOutlined />
                    </LoadingWrapper>
                ),
                value: 'loading',
            },
        ];
    }
    return entities.map((entity) => ({
        label: (
            <>
                <ParentEntities parentEntities={getParentDomains(entity, entityRegistry)} />
                {entityRegistry.getDisplayName(entity.type, entity)}
            </>
        ),
        value: entity.urn,
    }));
}

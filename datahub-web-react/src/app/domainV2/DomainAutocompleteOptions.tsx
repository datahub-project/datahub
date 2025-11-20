import { LoadingOutlined } from '@ant-design/icons';
import React from 'react';
import styled from 'styled-components';

import { getParentDomains } from '@app/domain/utils';
import EntityRegistry from '@app/entity/EntityRegistry';
import { ANTD_GRAY } from '@app/entityV2/shared/constants';
import { DomainColoredIcon } from '@app/entityV2/shared/links/DomainColoredIcon';
import ParentEntities from '@app/search/filters/ParentEntities';

import { Domain, Entity } from '@types';

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
const LabelWrapper = styled.div`
    display: flex;
    align-items: center;
    flex-direction: row;
`;
const LabelContent = styled.div`
    display: flex;
    flex-direction: column;
    margin-left: 8px;
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
            <LabelWrapper>
                <DomainColoredIcon domain={entity as Domain} size={24} fontSize={12} />
                <LabelContent>
                    {entityRegistry.getDisplayName(entity.type, entity)}
                    <ParentEntities hideIcon parentEntities={getParentDomains(entity, entityRegistry)} />
                </LabelContent>
            </LabelWrapper>
        ),
        value: entity.urn,
    }));
}

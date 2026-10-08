import { Loader } from '@components';
import React from 'react';
import styled from 'styled-components';

import { getParentDomains } from '@app/domain/utils';
import EntityRegistry from '@app/entity/EntityRegistry';
import { DomainColoredIcon } from '@app/entityV2/shared/links/DomainColoredIcon';
import ParentEntities from '@app/search/filters/ParentEntities';

import { Domain, Entity } from '@types';

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
                label: <Loader size="xs" padding={8} />,
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

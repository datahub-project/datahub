import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

import EntityIcon from '@app/entity/shared/components/styled/EntityIcon';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { Entity } from '@types';

const SelectedEntityWrapper = styled.div`
    display: flex;
    align-items: center;
    font-size: 14px;
    overflow: hidden;
`;

const IconWrapper = styled.span`
    margin-right: 4px;
    display: flex;
`;

const NameWrapper = styled(Typography.Text)`
    margin-right: 4px;
`;

interface Props {
    entity: Entity;
}

export default function SelectedEntity({ entity }: Props) {
    const entityRegistry = useEntityRegistry();
    const displayName = entityRegistry.getDisplayName(entity.type, entity);

    return (
        <SelectedEntityWrapper>
            <IconWrapper>
                <EntityIcon entity={entity} />
            </IconWrapper>
            <NameWrapper ellipsis={{ tooltip: displayName }}>{displayName}</NameWrapper>
        </SelectedEntityWrapper>
    );
}

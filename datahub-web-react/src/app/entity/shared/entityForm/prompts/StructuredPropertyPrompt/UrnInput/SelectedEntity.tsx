/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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

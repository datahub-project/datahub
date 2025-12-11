/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';
import styled from 'styled-components';

import { IconStyleType } from '@app/entity/Entity';
import { useEntityRegistry } from '@app/useEntityRegistry';

type Props = {
    entity: any;
};

const Container = styled.div`
    display: flex;
    justify-content: left;
    align-items: center;
    padding: 12px;
`;

const IconContainer = styled.div`
    margin-right: 8px;
`;

export const EntitySearchInputResult = ({ entity }: Props) => {
    const entityRegistry = useEntityRegistry();
    return (
        <Container>
            <IconContainer>{entityRegistry.getIcon(entity.type, 12, IconStyleType.ACCENT)}</IconContainer>
            {entityRegistry.getDisplayName(entity.type, entity)}
        </Container>
    );
};

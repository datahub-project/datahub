import React from 'react';
import styled from 'styled-components';

import { IconStyleType } from '@app/entityV2/Entity';
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

export default EntitySearchInputResult;

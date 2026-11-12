import { Text } from '@components';
import React from 'react';
import styled from 'styled-components';

import { isDomain } from '@app/entityV2/domain/utils';
import { DomainColoredIcon } from '@app/entityV2/shared/links/DomainColoredIcon';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { Entity, EntityType } from '@types';

const Container = styled.div`
    display: flex;
    flex-direction: row;
    gap: 8px;
    align-items: center;
`;

interface Props {
    entity: Entity;
}

export function DomainSelectOption({ entity }: Props) {
    const entityRegistry = useEntityRegistry();

    const domain = (isDomain(entity) && entity) || undefined;
    if (domain === undefined) return null;

    const displayName = entityRegistry.getDisplayName(EntityType.Domain, domain);

    return (
        <Container>
            <DomainColoredIcon domain={domain} size={24} fontSize={16} />
            <Text color="text" weight="medium">
                {displayName}
            </Text>
        </Container>
    );
}

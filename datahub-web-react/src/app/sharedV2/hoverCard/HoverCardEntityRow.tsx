import { Text } from '@components';
import React from 'react';
import styled from 'styled-components';

import EntityIcon from '@app/searchV2/autoCompleteV2/components/icon/EntityIcon';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';
import { Entity } from '@src/types.generated';

const Row = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
    min-width: 0;
`;

const IconSlot = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    flex-shrink: 0;
`;

const NameBlock = styled.div`
    display: flex;
    flex-direction: column;
    min-width: 0;
`;

const Name = styled.div`
    color: ${(props) => props.theme.colors.text};
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
`;

const Type = styled.div`
    color: ${(props) => props.theme.colors.textTertiary};
`;

type Props = {
    entity: Entity;
};

export default function HoverCardEntityRow({ entity }: Props) {
    const entityRegistry = useEntityRegistryV2();

    return (
        <Row>
            <IconSlot>
                <EntityIcon entity={entity} size={24} />
            </IconSlot>
            <NameBlock>
                <Name>
                    <Text size="md" weight="semiBold" lineHeight="sm">
                        {entityRegistry.getDisplayName(entity.type, entity)}
                    </Text>
                </Name>
                <Type>
                    <Text size="md" lineHeight="sm">
                        {entityRegistry.getEntityName(entity.type)}
                    </Text>
                </Type>
            </NameBlock>
        </Row>
    );
}

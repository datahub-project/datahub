import { Text } from '@components';
import React from 'react';
import styled from 'styled-components';

import EntityIcon from '@app/searchV2/autoCompleteV2/components/icon/EntityIcon';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

import { CorpUser, Entity, EntityType } from '@types';

const Container = styled.div`
    display: flex;
    flex-direction: row;
    gap: 8px;
    text-overflow: ellipsis;
    text-wrap: nowrap;
    align-items: center;
    justify-content: space-between;
`;

const IconAndNameContainer = styled.div`
    display: flex;
    flex-direction: row;
    gap: 8px;
    text-overflow: ellipsis;
    text-wrap: nowrap;
    align-items: center;
`;

const IconWrapper = styled.div`
    display: flex;
    align-items: center;

    & .ant-image {
        display: flex;
        align-items: center;
    }
`;

const TitleContainer = styled.div`
    display: flex;
    flex-direction: column;
`;

interface Props {
    entity: Entity;
}

export default function OwnerOption({ entity }: Props) {
    const entityRegistry = useEntityRegistryV2();

    const displayName = entityRegistry.getDisplayName(entity.type, entity);
    const subtitle = entity.type === EntityType.CorpUser ? (entity as CorpUser)?.properties?.email : undefined;

    return (
        <Container>
            <IconAndNameContainer>
                <IconWrapper>
                    <EntityIcon entity={entity} />
                </IconWrapper>
                <TitleContainer>
                    <Text type="div">{displayName}</Text>
                    {subtitle && (
                        <Text type="div" size="sm" color="gray">
                            {subtitle}
                        </Text>
                    )}
                </TitleContainer>
            </IconAndNameContainer>
        </Container>
    );
}

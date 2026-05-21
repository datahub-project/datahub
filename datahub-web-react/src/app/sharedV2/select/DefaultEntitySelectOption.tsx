import { Text } from '@components';
import React from 'react';
import styled from 'styled-components';

import { SingleEntityIcon } from '@app/searchV2/autoCompleteV2/components/icon/SingleEntityIcon';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { Entity } from '@types';

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
    padding-right: 8px;
`;

const IconWrapper = styled.div`
    display: flex;
    align-items: center;

    & .ant-image {
        display: flex;
        align-items: center;
    }
`;

const Name = styled.div`
    max-width: 250px;
    text-overflow: ellipsis;
    overflow: hidden;
`;

const DisplayName = styled(Text)`
    color: ${(props) => props.theme.colors.textSecondary};
`;

interface Props {
    entity?: Entity;
}

export function DefaultEntitySelectOption({ entity }: Props) {
    const entityRegistry = useEntityRegistry();

    if (!entity) return null;

    const displayName = entityRegistry.getDisplayName(entity.type, entity);

    return (
        <Container>
            <IconAndNameContainer>
                <IconWrapper>
                    <SingleEntityIcon entity={entity} size={16} />
                </IconWrapper>
                <Name>
                    <DisplayName>{displayName}</DisplayName>
                </Name>
            </IconAndNameContainer>
        </Container>
    );
}

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

import { Text } from '@src/alchemy-components';
import { SingleEntityIcon } from '@src/app/searchV2/autoCompleteV2/components/icon/SingleEntityIcon';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';
import { Entity } from '@src/types.generated';

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

interface Props {
    entity?: Entity;
}

export function EntityIconWithName({ entity }: Props) {
    const entityRegistry = useEntityRegistryV2();

    if (!entity) return null;

    const displayName = entityRegistry.getDisplayName(entity.type, entity);

    return (
        <Container>
            <IconAndNameContainer>
                <IconWrapper>
                    <SingleEntityIcon entity={entity} size={16} />
                </IconWrapper>
                <Name>
                    <Text type="span">{displayName}</Text>
                </Name>
            </IconAndNameContainer>
        </Container>
    );
}

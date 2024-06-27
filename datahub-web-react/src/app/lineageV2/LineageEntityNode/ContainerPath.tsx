import { ANTD_GRAY } from '@app/entityV2/shared/constants';
import { ContainerIconBase } from '@app/entityV2/shared/containers/profile/header/PlatformContent/ContainerIcon';
import { FetchedEntityV2 } from '@app/lineageV2/types';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { EntityType } from '@types';
import React from 'react';
import styled from 'styled-components';

const ContainerPathWrapper = styled.div`
    display: flex;
    height: 12px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    width: 100%;
`;

// TODO: Put ellipsis on last item correctly
const ContainerEntry = styled.div<{ numItems?: number; isLast: boolean }>`
    align-items: center;
    color: ${ANTD_GRAY[9]};
    display: flex;
    flex-direction: row;
    font-size: 12px;
    max-width: ${({ numItems, isLast }) => (numItems && !isLast ? 100 / numItems : 100)}%;
`;

const ContainerText = styled.span`
    font-size: 8px;
    margin-left: 4px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
`;

const VerticalDivider = styled.hr<{ margin: number }>`
    align-self: stretch;
    height: auto;
    margin: 0 ${({ margin }) => margin}px;
    border: 0.5px solid;
    opacity: 0.1;
    vertical-align: text-top;
`;

export default function ContainerPath({ parentContainers }: Pick<FetchedEntityV2, 'parentContainers'>) {
    const entityRegistry = useEntityRegistry();
    const containers = parentContainers?.slice(0, 1);

    if (!containers?.length) {
        return null;
    }

    return (
        <ContainerPathWrapper>
            {containers?.map((container, i) => (
                <ContainerEntry key={container.urn} isLast={i === containers.length - 1} numItems={containers.length}>
                    {i > 0 && <VerticalDivider margin={4} />}
                    <ContainerIconBase container={container} />
                    <ContainerText>{entityRegistry.getDisplayName(EntityType.Container, container)}</ContainerText>
                </ContainerEntry>
            ))}
        </ContainerPathWrapper>
    );
}

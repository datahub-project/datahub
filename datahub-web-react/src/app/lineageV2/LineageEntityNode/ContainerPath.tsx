import { GenericEntityProperties } from '@app/entity/shared/types';
import { ANTD_GRAY } from '@app/entityV2/shared/constants';
import { ContainerIconBase } from '@app/entityV2/shared/containers/profile/header/PlatformContent/ContainerIcon';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { Container } from '@types';
import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

const ContainerPathWrapper = styled.div`
    display: flex;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    width: 100%;
`;

// TODO: Put ellipsis on last item correctly
const ContainerEntry = styled.div<{ numItems?: number; isLast: boolean }>`
    align-items: center;
    color: ${ANTD_GRAY[9]};
    font-size: 10px;
    display: flex;
    flex-direction: row;
    max-width: ${({ numItems, isLast }) => (numItems && !isLast ? 100 / numItems : 100)}%;
`;

const ContainerText = styled(Typography.Text)`
    color: inherit;
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

interface Props {
    parents?: Container[] | GenericEntityProperties[];
    className?: string;
}

export default function ContainerPath({ parents, className }: Props) {
    const entityRegistry = useEntityRegistry();
    const containers = parents?.slice(0, 1);

    if (!containers?.length) {
        return null;
    }

    return (
        <ContainerPathWrapper className={className}>
            {containers?.map((container, i) => (
                <ContainerEntry key={container.urn} isLast={i === containers.length - 1} numItems={containers.length}>
                    {i > 0 && <VerticalDivider margin={4} />}
                    <ContainerIconBase container={container} />
                    <ContainerText ellipsis={{ tooltip: true }}>
                        {entityRegistry.getDisplayName(container.type, container)}
                    </ContainerText>
                </ContainerEntry>
            ))}
        </ContainerPathWrapper>
    );
}

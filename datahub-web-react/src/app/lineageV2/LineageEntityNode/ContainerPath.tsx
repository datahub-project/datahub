import { GenericEntityProperties } from '@app/entity/shared/types';
import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';
import { ContainerIconBase } from '@app/entityV2/shared/containers/profile/header/PlatformContent/ContainerIcon';
import MatchTextSizeWrapper from '@app/sharedV2/text/MatchTextSizeWrapper';
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

    flex: 1 1 10px;
    min-height: 8px;
    max-height: 12px;
`;

const EmptyContainer = styled.div`
    height: 8px;
`;

// TODO: Put ellipsis on last item correctly
const ContainerEntry = styled(MatchTextSizeWrapper)<{ numItems?: number; isLast: boolean }>`
    align-items: center;
    color: ${REDESIGN_COLORS.TEXT_GREY};
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
        return <EmptyContainer />;
    }

    return (
        <ContainerPathWrapper className={className}>
            {containers?.map((container, i) => (
                <ContainerEntry
                    key={container.urn}
                    isLast={i === containers.length - 1}
                    numItems={containers.length}
                    defaultHeight={10}
                >
                    {i > 0 && <VerticalDivider margin={4} />}
                    <ContainerIconBase container={container} />
                    <ContainerText ellipsis={{ tooltip: { showArrow: false } }}>
                        {/*
                            Browse paths with no entity are stored as { name: ... }, with no type.
                            Entity registry will return empty string display name for undefined type.
                         */}
                        {entityRegistry.getDisplayName(container.type, container) || container.name}
                    </ContainerText>
                </ContainerEntry>
            ))}
        </ContainerPathWrapper>
    );
}

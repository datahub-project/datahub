import { FolderOpenOutlined } from '@ant-design/icons';
import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components/macro';
import { Container, EntityType } from '../../../types.generated';
import { useEntityRegistry } from '../../useEntityRegistry';
import { ANTD_GRAY } from '../../entity/shared/constants';

const NUM_VISIBLE_CONTAINERS = 2;

const ParentContainersWrapper = styled.div`
    font-size: 12px;
    color: ${ANTD_GRAY[9]};
    display: flex;
    align-items: center;
    margin-bottom: 3px;
`;

const ParentContainer = styled(Typography.Text)`
    margin-left: 4px;
`;

export const ArrowWrapper = styled.span`
    margin: 0 3px;
`;

interface Props {
    parentContainers: Container[];
}

export default function ParentContainers({ parentContainers }: Props) {
    const entityRegistry = useEntityRegistry();

    const visibleContainers = parentContainers.slice(parentContainers.length - NUM_VISIBLE_CONTAINERS);
    const numHiddenContainers = parentContainers.length - NUM_VISIBLE_CONTAINERS;

    return (
        <ParentContainersWrapper>
            {numHiddenContainers > 0 &&
                [...Array(numHiddenContainers)].map(() => (
                    <>
                        <FolderOpenOutlined />
                        <ArrowWrapper>{'>'}</ArrowWrapper>
                    </>
                ))}
            {visibleContainers.map((container, index) => (
                <>
                    <FolderOpenOutlined />
                    <ParentContainer ellipsis={{ tooltip: '' }}>
                        {entityRegistry.getDisplayName(EntityType.Container, container)}
                    </ParentContainer>
                    {index !== visibleContainers.length - 1 && <ArrowWrapper>{'>'}</ArrowWrapper>}
                </>
            ))}
        </ParentContainersWrapper>
    );
}

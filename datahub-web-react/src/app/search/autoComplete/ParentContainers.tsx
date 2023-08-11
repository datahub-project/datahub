import { FolderOpenOutlined } from '@ant-design/icons';
import { Typography } from 'antd';
import React, { Fragment } from 'react';
import styled from 'styled-components/macro';
import { Container, EntityType } from '../../../types.generated';
import { useEntityRegistry } from '../../useEntityRegistry';
import { ANTD_GRAY_V2 } from '../../entity/shared/constants';

const NUM_VISIBLE_CONTAINERS = 2;

const ParentContainersWrapper = styled.div`
    font-size: 12px;
    color: ${ANTD_GRAY_V2[8]};
    display: flex;
    align-items: center;
`;

const ParentContainer = styled(Typography.Text)`
    color: ${ANTD_GRAY_V2[8]};
    margin-left: 4px;
    font-weight: 500;
`;

export const ArrowWrapper = styled.span`
    margin: 0 3px;
`;

interface Props {
    parentContainers: Container[];
}

export default function ParentContainers({ parentContainers }: Props) {
    const entityRegistry = useEntityRegistry();

    const visibleIndex = Math.max(parentContainers.length - NUM_VISIBLE_CONTAINERS, 0);
    const visibleContainers = parentContainers.slice(visibleIndex);
    const hiddenContainers = parentContainers.slice(0, visibleIndex);

    return (
        <ParentContainersWrapper>
            {hiddenContainers.map((container) => (
                <Fragment key={container.urn}>
                    <FolderOpenOutlined />
                    <ArrowWrapper>{'>'}</ArrowWrapper>
                </Fragment>
            ))}
            {visibleContainers.map((container, index) => (
                <Fragment key={container.urn}>
                    <FolderOpenOutlined />
                    <ParentContainer ellipsis={{ tooltip: '' }}>
                        {entityRegistry.getDisplayName(EntityType.Container, container)}
                    </ParentContainer>
                    {index !== visibleContainers.length - 1 && <ArrowWrapper>{'>'}</ArrowWrapper>}
                </Fragment>
            ))}
        </ParentContainersWrapper>
    );
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { FolderOpenOutlined } from '@ant-design/icons';
import { Typography } from 'antd';
import React, { Fragment } from 'react';
import styled from 'styled-components/macro';

import { ANTD_GRAY_V2 } from '@app/entity/shared/constants';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { Container, EntityType } from '@types';

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

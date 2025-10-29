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

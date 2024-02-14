import * as React from 'react';
import { Skeleton, Space } from 'antd';
import styled from 'styled-components';
import { ANTD_GRAY } from '../../../constants';

const NameSkeleton = styled(Skeleton.Input)`
    && {
        width: 240px;
        border-radius: 4px;
        background-color: ${ANTD_GRAY[3]};
    }
`;

export default function EntityTitleLoadingSection() {
    return (
        <Space direction="vertical">
            <NameSkeleton active size="default" />
        </Space>
    );
}

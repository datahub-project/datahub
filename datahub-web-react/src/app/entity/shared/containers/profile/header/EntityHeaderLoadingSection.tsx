import * as React from 'react';
import { Skeleton, Space } from 'antd';
import styled from 'styled-components';
import { ANTD_GRAY } from '../../../constants';

const ContextSkeleton = styled(Skeleton.Input)`
    && {
        width: 320px;
        border-radius: 4px;
        background-color: ${ANTD_GRAY[3]};
    }
`;

const NameSkeleton = styled(Skeleton.Input)`
    && {
        width: 240px;
        border-radius: 4px;
        background-color: ${ANTD_GRAY[3]};
    }
`;

export default function EntityHeaderLoadingSection() {
    return (
        <Space direction="vertical">
            <ContextSkeleton active size="small" />
            <NameSkeleton active size="default" />
        </Space>
    );
}

import * as React from 'react';
import { Skeleton, Space } from 'antd';
import styled from 'styled-components';
import { ANTD_GRAY } from '../../shared/constants';

const TypeSkeleton = styled(Skeleton.Input)`
    && {
        width: 60px;
        height: 60px;
        border-radius: 8px;
        background-color: ${ANTD_GRAY[3]};
    }
`;

export default function ContentSectionLoading() {
    return (
        <Space direction="horizontal">
            <TypeSkeleton active size="default" />
            <TypeSkeleton active size="default" />
            <TypeSkeleton active size="default" />
            <TypeSkeleton active size="default" />
        </Space>
    );
}

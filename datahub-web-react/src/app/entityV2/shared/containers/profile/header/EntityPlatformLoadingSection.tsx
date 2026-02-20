import { Skeleton, Space } from 'antd';
import * as React from 'react';
import styled from 'styled-components';

const ContextSkeleton = styled(Skeleton.Input)`
    && {
        width: 320px;
        border-radius: 4px;
        background-color: ${(props) => props.theme.colors.bgSurface};
    }
`;

export default function EntityPlatformLoadingSection() {
    return (
        <Space direction="vertical">
            <ContextSkeleton active size="small" />
        </Space>
    );
}

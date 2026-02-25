import { Skeleton, Space } from 'antd';
import * as React from 'react';
import styled from 'styled-components';

const TypeSkeleton = styled(Skeleton.Input)`
    && {
        width: 60px;
        height: 60px;
        border-radius: 8px;
        background-color: ${(props) => props.theme.colors.bgSurface};
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

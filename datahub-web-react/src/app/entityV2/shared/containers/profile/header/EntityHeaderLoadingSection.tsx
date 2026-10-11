import { Skeleton, Space } from 'antd';
import * as React from 'react';
import styled from 'styled-components';

const Wrapper = styled(Space)`
    min-height: 50px;
    width: 100%;
`;

const NameSkeleton = styled(Skeleton.Input)`
    && {
        height: 20px;
        width: 240px;
        border-radius: 4px;
        background-color: ${(props) => props.theme.colors.bgSurface};
        margin-right: 12px;
    }
`;

export default function EntityTitleLoadingSection() {
    return (
        <Wrapper direction="horizontal">
            <Skeleton.Avatar active />

            <Space direction="vertical">
                <NameSkeleton active />
                <NameSkeleton active />
            </Space>
        </Wrapper>
    );
}

import * as React from 'react';
import { Skeleton, Space } from 'antd';
import styled from 'styled-components';
import { ANTD_GRAY } from '../../../constants';

const Wrapper = styled(Space)`
    min-height: 50px;
    width: 100%;
`;

const NameSkeleton = styled(Skeleton.Input)`
    && {
        height: 20px;
        width: 240px;
        border-radius: 4px;
        background-color: ${ANTD_GRAY[3]};
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

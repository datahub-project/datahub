import React from 'react';
import { Spin } from 'antd';
import styled from 'styled-components';
import { LoadingOutlined } from '@ant-design/icons';
import { ANTD_GRAY } from '../entity/shared/constants';

const ProtectedLoadingWrapper = styled.div`
    display: flex;
    justify-content: center;
    align-items: center;
    height: 100vh;
`;

export const ProtectedRouteLoadingIndicator = () => {
    return (
        <ProtectedLoadingWrapper>
            <Spin indicator={<LoadingOutlined style={{ color: ANTD_GRAY[7] }} />} />
        </ProtectedLoadingWrapper>
    );
};

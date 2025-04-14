import { Skeleton as AntdSkeleton } from 'antd';
import React from 'react';
import styled from 'styled-components';

const SkeletonContainer = styled.div`
    height: 40px;
    width: 100%;
    max-width: 620px;
`;

const SkeletonButton = styled(AntdSkeleton.Button)`
    &&& {
        height: inherit;
        width: inherit;
    }
`;

export default function Skeleton() {
    return (
        <SkeletonContainer>
            <SkeletonButton shape="square" active block />
        </SkeletonContainer>
    );
}

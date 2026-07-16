import { Skeleton } from 'antd';
import React from 'react';
import styled from 'styled-components';

const Wrapper = styled.div`
    display: flex;
    flex-direction: column;
    padding: 8px;
    gap: 16px;
    width: 100%;
`;

const SkeletonRow = styled(Skeleton.Input)`
    && {
        width: 100%;
    }
`;

export default function TreeViewSkeleton() {
    return (
        <Wrapper>
            <SkeletonRow active size="small" />
            <SkeletonRow active size="small" />
            <SkeletonRow active size="small" />
        </Wrapper>
    );
}

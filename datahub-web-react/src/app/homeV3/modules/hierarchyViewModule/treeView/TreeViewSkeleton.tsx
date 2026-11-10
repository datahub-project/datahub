import { Skeleton } from '@components';
import React from 'react';
import styled from 'styled-components';

const Wrapper = styled.div`
    display: flex;
    flex-direction: column;
    padding: 8px;
    gap: 16px;
    width: 100%;
`;

export default function TreeViewSkeleton() {
    return (
        <Wrapper>
            <Skeleton.Input active size="sm" block />
            <Skeleton.Input active size="sm" block />
            <Skeleton.Input active size="sm" block />
        </Wrapper>
    );
}

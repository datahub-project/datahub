import { Col, Row, Skeleton } from 'antd';
import React from 'react';
import styled from 'styled-components';

const Container = styled(Row)`
    display: flex;
    flex-direction: column;
    justify-content: start;
    gap: 8px;
    margin: 0;
    width: 100%;
`;

const SkeletonCol = styled(Col)`
    margin-top: 8px;
    display: flex;
    align-items: center;
    gap: 0.5em;
`;

const SkeletonText = styled(Skeleton.Button)`
    &&& {
        border-radius: 4px;
        height: 22px;
        width: 10rem;
    }
`;

const SkeletonButton = styled(Skeleton.Button)`
    &&& {
        border-radius: 11px;
        height: 60px;
        width: 100%;
    }
`;

export const PendingTasksSkeleton = () => {
    return (
        <Container>
            <SkeletonCol>
                <Skeleton.Avatar active size="small" shape="circle" />
                <SkeletonText active size="small" shape="square" block />
            </SkeletonCol>
            <SkeletonCol>
                <SkeletonButton active size="small" shape="square" block />
            </SkeletonCol>
        </Container>
    );
};

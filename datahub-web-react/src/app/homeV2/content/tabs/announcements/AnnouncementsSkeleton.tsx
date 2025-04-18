import React from 'react';
import { Col, Row, Skeleton } from 'antd';
import styled from 'styled-components';

const Container = styled(Row)`
    display: flex;
    flex-direction: column;
    justify-content: start;
    gap: 8px;
    margin-left: 12px 0px 12px 0px;
`;

const SkeletonCol = styled(Col)`
    margin-bottom: 5px;
    display: flex;
    align-items: center;
    gap: 1rem;
`;

const SkeletonButton = styled(Skeleton.Button)<{ width?: string }>`
    &&& {
        width: ${(props) => (props.width ? props.width : '100%')};
        border-radius: 4px;
        height: 63px;
    }
`;

export default function AnnouncementsSkeleton() {
    return (
        <Container>
            <SkeletonCol>
                <Skeleton.Avatar active size="small" shape="circle" />
                <SkeletonButton active size="small" shape="square" block width="10rem" />
            </SkeletonCol>
            <SkeletonCol>
                <SkeletonButton active size="small" shape="square" block />
            </SkeletonCol>
            <SkeletonCol>
                <SkeletonButton active size="small" shape="square" block />
            </SkeletonCol>
            <SkeletonCol>
                <SkeletonButton active size="small" shape="square" block />
            </SkeletonCol>
        </Container>
    );
}

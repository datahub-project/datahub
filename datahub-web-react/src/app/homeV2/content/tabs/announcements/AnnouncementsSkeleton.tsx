import { Skeleton } from '@components';
import React from 'react';
import styled from 'styled-components';

const Container = styled.div`
    min-width: 0;
    display: flex;
    flex-direction: column;
    justify-content: start;
    gap: 8px;
    margin-left: 12px 0px 12px 0px;
`;

const SkeletonCol = styled.div`
    position: relative;
    max-width: 100%;
    min-height: 1px;
    margin-bottom: 5px;
    display: flex;
    align-items: center;
    gap: 1rem;
`;

const SkeletonButton = styled(Skeleton.Button)`
    &&& {
        border-radius: 4px;
        height: 63px;
    }
`;

export default function AnnouncementsSkeleton() {
    return (
        <Container>
            <SkeletonCol>
                <Skeleton.Avatar active size="sm" shape="circle" />
                <SkeletonButton active size="sm" shape="square" block width="10rem" />
            </SkeletonCol>
            <SkeletonCol>
                <SkeletonButton active size="sm" shape="square" block />
            </SkeletonCol>
            <SkeletonCol>
                <SkeletonButton active size="sm" shape="square" block />
            </SkeletonCol>
            <SkeletonCol>
                <SkeletonButton active size="sm" shape="square" block />
            </SkeletonCol>
        </Container>
    );
}

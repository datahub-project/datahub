import { Skeleton } from '@components';
import React from 'react';
import styled from 'styled-components';

const SkeletonContainer = styled.div`
    display: flex;
    flex-flow: row wrap;
    min-width: 0;
    margin-bottom: 20px;
    border-bottom: 1px solid ${(props) => props.theme.colors.border};
`;

const SkeletonCol = styled.div`
    position: relative;
    max-width: 100%;
    min-height: 1px;
    margin-bottom: 20px;
    width: 100%;
    display: flex;
    align-items: center;
    gap: 1rem;
`;

export const EntityLinkListSkeleton = () => {
    return (
        <SkeletonContainer>
            <SkeletonCol>
                <Skeleton.Button active size="sm" shape="square" width="10rem" height={16} />
                <Skeleton.Avatar active size={16} shape="circle" />
            </SkeletonCol>

            <SkeletonCol>
                <Skeleton.Avatar active size="sm" shape="square" />
                <Skeleton.Button active size="sm" shape="square" width="12rem" height={16} />
            </SkeletonCol>
            <SkeletonCol>
                <Skeleton.Avatar active size="sm" shape="square" />
                <Skeleton.Button active size="sm" shape="square" width="5rem" height={16} />
            </SkeletonCol>
        </SkeletonContainer>
    );
};

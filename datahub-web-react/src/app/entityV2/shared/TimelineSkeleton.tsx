import { Skeleton } from '@components';
import React from 'react';
import styled from 'styled-components';

const TimelineWrapper = styled.div`
    display: flex;
    flex-direction: column;
    gap: 24px;
    margin-top: 8px;
`;

const ItemWrapper = styled.div`
    display: flex;
    flex-direction: row;
    gap: 16px;
`;

const ContentWrapper = styled.div`
    display: flex;
    flex-direction: column;
    gap: 2px;
`;

export default function TimelineSkeleton() {
    const renderSkeletonItem = () => (
        <ItemWrapper>
            <Skeleton.Avatar />
            <ContentWrapper>
                <Skeleton.Button width={150} height={14} />
                <Skeleton.Button width={150} height={12} />
            </ContentWrapper>
        </ItemWrapper>
    );

    return (
        <TimelineWrapper>
            {renderSkeletonItem()}
            {renderSkeletonItem()}
            {renderSkeletonItem()}
            {renderSkeletonItem()}
            {renderSkeletonItem()}
        </TimelineWrapper>
    );
}

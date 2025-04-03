import { Skeleton } from 'antd';
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

const SkeletonContentLine = styled(Skeleton.Button)`
    .ant-skeleton-button {
        height: 14px;
        width: 150px;
        margin: 0;
    }
`;

const SkeletonContentTimestampLine = styled(Skeleton.Button)`
    .ant-skeleton-button {
        height: 12px;
        width: 150px;
        margin: 0;
    }
`;

export default function TimelineSkeleton() {
    const renderSkeletonItem = () => (
        <ItemWrapper>
            <Skeleton.Avatar active />
            <ContentWrapper>
                <SkeletonContentLine active />
                <SkeletonContentTimestampLine active />
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

import React from 'react';
import { Skeleton } from 'antd';
import styled from 'styled-components';

const CardHeader = styled.div`
    display: flex;
    gap: 16px;
    padding: 16px 0;

    .ant-skeleton {
        width: auto;
        height: 16px;
    }

    .ant-skeleton-button {
        height: 16px;
        background-color: #ebecf0;
    }
`;

const CardContent = styled.div`
    display: flex;
    gap: 10px;
    flex-direction: column;

    .ant-skeleton-button {
        height: 16px;
        background-color: #ebecf0;
    }

    .ant-skeleton-avatar-square {
        height: 24px;
        width: 24px;
    }
`;

const Row = styled.div`
    display: flex;
    gap: 10px;
    align-items: center;
`;

const CardSkeleton = styled.div`
    flex-shrink: 0;

    display: flex;
    flex-direction: column;
    gap: 16px;
    border-radius: 12px;
    background-color: #f8f9fa;
    padding: 16px;
    width: 340px;
    height: 275px;
`;

const SkeletonButton = styled(Skeleton.Button)<{ width: string }>`
    &&& {
        height: 25px;
        width: ${(props) => props.width};
    }
`;

const InsightCardSkeleton = () => {
    return (
        <CardSkeleton>
            <CardHeader>
                <SkeletonButton active size="small" shape="square" block width="8rem" />
                <Skeleton.Avatar active size="small" shape="circle" />
            </CardHeader>
            <CardContent>
                <Row>
                    <Skeleton.Avatar active size="small" shape="square" />
                    <SkeletonButton active size="small" shape="square" block width="8rem" />
                </Row>
                <Row>
                    <Skeleton.Avatar active size="small" shape="square" />
                    <SkeletonButton active size="small" shape="square" block width="5rem" />
                </Row>
                <Row>
                    <Skeleton.Avatar active size="small" shape="square" />
                    <SkeletonButton active size="small" shape="square" block width="6rem" />
                </Row>
                <Row>
                    <Skeleton.Avatar active size="small" shape="square" />
                    <SkeletonButton active size="small" shape="square" block width="7rem" />
                </Row>
            </CardContent>
        </CardSkeleton>
    );
};

export default InsightCardSkeleton;

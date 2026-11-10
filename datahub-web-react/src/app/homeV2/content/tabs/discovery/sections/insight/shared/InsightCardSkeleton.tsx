import { Skeleton } from '@components';
import React from 'react';
import styled from 'styled-components';

const CardHeader = styled.div`
    display: flex;
    gap: 16px;
    padding: 16px 0;
`;

const CardContent = styled.div`
    display: flex;
    gap: 10px;
    flex-direction: column;
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
    background-color: ${(props) => props.theme.colors.bgSurface};
    padding: 16px;
    width: 340px;
    height: 275px;
`;

const InsightCardSkeleton = () => {
    return (
        <CardSkeleton>
            <CardHeader>
                <Skeleton.Button active size="sm" shape="square" width="8rem" height={16} />
                <Skeleton.Avatar active size="sm" shape="circle" />
            </CardHeader>
            <CardContent>
                <Row>
                    <Skeleton.Avatar active size="sm" shape="square" />
                    <Skeleton.Button active size="sm" shape="square" width="8rem" height={16} />
                </Row>
                <Row>
                    <Skeleton.Avatar active size="sm" shape="square" />
                    <Skeleton.Button active size="sm" shape="square" width="5rem" height={16} />
                </Row>
                <Row>
                    <Skeleton.Avatar active size="sm" shape="square" />
                    <Skeleton.Button active size="sm" shape="square" width="6rem" height={16} />
                </Row>
                <Row>
                    <Skeleton.Avatar active size="sm" shape="square" />
                    <Skeleton.Button active size="sm" shape="square" width="7rem" height={16} />
                </Row>
            </CardContent>
        </CardSkeleton>
    );
};

export default InsightCardSkeleton;

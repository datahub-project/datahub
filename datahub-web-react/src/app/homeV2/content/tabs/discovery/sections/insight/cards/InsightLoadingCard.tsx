import React from 'react';
import { Skeleton } from 'antd';
import styled from 'styled-components';

const Container = styled.div``;

const CardSkeleton = styled(Skeleton.Input)`
    && {
        width: 340px;
        height: 270px;
        border-radius: 10px;
    }
`;

export const InsightLoadingCard = () => {
    return (
        <Container>
            <CardSkeleton active size="large" />
        </Container>
    );
};

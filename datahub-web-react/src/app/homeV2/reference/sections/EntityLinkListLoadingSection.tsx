import React from 'react';
import styled from 'styled-components';
import { Skeleton } from 'antd';

const Container = styled.div`
    display: flex;
    flex-direction: column;
    justify-content: start;
    gap: 8px;
    margin-left: 12px 0px 12px 0px;
`;

const CardSkeleton = styled(Skeleton.Input)`
    && {
        padding: 2px 12px 2px 0px;
        height: 20px;
        border-radius: 8px;
        width: 100%;
    }
`;

export const EntityLinkListLoadingSection = () => {
    return (
        <Container>
            <CardSkeleton active size="default" />
            <CardSkeleton active size="default" />
            <CardSkeleton active size="default" />
        </Container>
    );
};

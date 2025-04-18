import React from 'react';
import styled from 'styled-components';
import { Card } from 'antd';

const Body = styled.div`
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(400px, 1fr));
    padding: 24px;
    row-gap: 24px;
    column-gap: 24px;
    overflow: auto;
`;

const CardSkeleton = styled(Card)`
    && {
        padding: 0px 12px 12px 0px;
        height: 210px;
        border-radius: 8px;
        width: 100%;
    }
`;

export const AcrylAssertionsSummaryTabLoading = () => {
    return (
        <Body>
            <CardSkeleton loading />
            <CardSkeleton loading />
            <CardSkeleton loading />
            <CardSkeleton loading />
        </Body>
    );
};

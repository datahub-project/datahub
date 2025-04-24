import { Skeleton } from 'antd';
import React from 'react';
import styled from 'styled-components';

const CardSkeleton = styled(Skeleton.Input)`
    && {
        padding: 2px 12px 2px 0px;
        height: 20px;
        border-radius: 8px;
        width: 100%;
    }
`;

export const TaskSummaryCardLoading = () => {
    return <CardSkeleton active size="large" />;
};

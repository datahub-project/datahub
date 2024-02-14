import React from 'react';
import styled from 'styled-components';
import { TaskSummaryCard } from './TaskSummaryCard';
import { formatNumber } from '../../../shared/formatNumber';

const Content = styled.span`
    text-wrap: wrap;
    padding: 12px 0px;
`;

type Props = {
    loading: boolean;
    count: number;
};

export const PendingRequests = ({ loading, count }: Props) => {
    const navigateToGovernanceCenter = () => {
        console.log('Not yet implemented: Navigate to gov center.');
    };

    return (
        <TaskSummaryCard loading={loading} onClick={navigateToGovernanceCenter}>
            <Content>
                <b>{formatNumber(count)} assets</b> with pending documentation requests
            </Content>
        </TaskSummaryCard>
    );
};

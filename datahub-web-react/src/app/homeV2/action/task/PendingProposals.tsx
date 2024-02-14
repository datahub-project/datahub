import React from 'react';
import styled from 'styled-components';
import { useHistory } from 'react-router';
import { TaskSummaryCard } from './TaskSummaryCard';
import { PageRoutes } from '../../../../conf/Global';
import { formatNumber } from '../../../shared/formatNumber';

const Content = styled.span`
    padding: 12px 0px;
    text-wrap: wrap;
`;

type Props = {
    loading: boolean;
    count: number;
};

export const PendingProposals = ({ loading, count }: Props) => {
    const history = useHistory();

    const navigateToInbox = () => {
        history.push(PageRoutes.ACTION_REQUESTS);
    };

    return (
        <TaskSummaryCard loading={loading} onClick={navigateToInbox}>
            <Content>
                <b>{formatNumber(count)} change proposals</b> pending review
            </Content>
        </TaskSummaryCard>
    );
};

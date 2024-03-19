import React from 'react';
import styled from 'styled-components';
import { useHistory } from 'react-router';
import { TaskSummaryCard } from './TaskSummaryCard';
import { PageRoutes } from '../../../../conf/Global';
import { formatNumber } from '../../../shared/formatNumber';
import analytics, { EventType } from '../../../analytics';

const Content = styled.span`
    padding: 12px 0px;
    text-wrap: wrap;
`;

type Props = {
    count: number;
};

export const PendingProposals = ({ count }: Props) => {
    const history = useHistory();

    const navigateToInbox = () => {
        history.push(PageRoutes.ACTION_REQUESTS);
        analytics.event({
            type: EventType.OpenTaskCenter,
        });
    };

    return (
        <TaskSummaryCard loading={false} onClick={navigateToInbox}>
            <Content>
                <b>{formatNumber(count)} change proposals</b> pending review
            </Content>
        </TaskSummaryCard>
    );
};

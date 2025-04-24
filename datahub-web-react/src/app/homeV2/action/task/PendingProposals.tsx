import React from 'react';
import { useHistory } from 'react-router';
import styled from 'styled-components';

import analytics, { EventType } from '@app/analytics';
import { TaskSummaryCard } from '@app/homeV2/action/task/TaskSummaryCard';
import { formatNumber } from '@app/shared/formatNumber';
import { PageRoutes } from '@conf/Global';

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

import React from 'react';
import { useHistory } from 'react-router';
import styled from 'styled-components';

import analytics, { EventType } from '@app/analytics';
import { TaskSummaryCard } from '@app/homeV2/action/task/TaskSummaryCard';
import { formatNumber } from '@app/shared/formatNumber';
import { PageRoutes } from '@conf/Global';

const Content = styled.span`
    text-wrap: wrap;
    padding: 12px 0px;
`;

type Props = {
    count: number;
};

export const PendingRequests = ({ count }: Props) => {
    const history = useHistory();

    const navigateToTaskCenter = () => {
        history.push(PageRoutes.ACTION_REQUESTS);
        analytics.event({
            type: EventType.OpenTaskCenter,
        });
    };

    return (
        <TaskSummaryCard loading={false} onClick={navigateToTaskCenter}>
            <Content>
                <b>{formatNumber(count)} assets</b> with pending compliance tasks
            </Content>
        </TaskSummaryCard>
    );
};

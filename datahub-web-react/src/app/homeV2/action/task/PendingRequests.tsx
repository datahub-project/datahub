import React from 'react';
import styled from 'styled-components';
import { useHistory } from 'react-router';
import { TaskSummaryCard } from './TaskSummaryCard';
import { PageRoutes } from '../../../../conf/Global';
import { formatNumber } from '../../../shared/formatNumber';
import analytics, { EventType } from '../../../analytics';

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

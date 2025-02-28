import React from 'react';
import { CheckCircleFilled, WarningFilled, StopOutlined } from '@ant-design/icons';
import { Typography } from 'antd';
import styled from 'styled-components';
import { ANTD_GRAY } from '../../../constants';
import { SUCCESS_COLOR_HEX, FAILURE_COLOR_HEX } from '../incidentUtils';

const SummaryHeader = styled.div`
    width: 100%;
    height: 80px;
    padding-left: 40px;
    padding-top: 0px;
    display: flex;
    align-items: center;
    padding-top: 20px;
    padding-bottom: 20px;
`;

const SummaryContainer = styled.div``;

const SummaryMessage = styled.div`
    display: inline-block;
    margin-left: 20px;
`;

const SummaryTitle = styled(Typography.Title)`
    && {
        padding-bottom: 0px;
        margin-bottom: 0px;
    }
`;

export type IncidentsSummary = {
    totalIncident: number;
    resolvedIncident: number;
    activeIncident: number;
};

type Props = {
    summary: IncidentsSummary;
};

const getSummaryIcon = (summary: IncidentsSummary) => {
    if (summary.totalIncident === 0) {
        return <StopOutlined style={{ color: ANTD_GRAY[6], fontSize: 28 }} />;
    }
    if (summary.resolvedIncident === summary.totalIncident) {
        return <CheckCircleFilled style={{ color: SUCCESS_COLOR_HEX, fontSize: 28 }} />;
    }
    return <WarningFilled style={{ color: FAILURE_COLOR_HEX, fontSize: 28 }} />;
};

const getSummaryMessage = (summary: IncidentsSummary) => {
    if (summary.totalIncident === 0) {
        return 'No incidents have been raised';
    }
    if (summary.resolvedIncident === summary.totalIncident) {
        return 'There are no active incidents';
    }
    if (summary.activeIncident === 1) {
        return `There is ${summary.activeIncident} active incident`;
    }
    if (summary.activeIncident > 1) {
        return `There are ${summary.activeIncident} active incidents`;
    }
    return null;
};

export const IncidentSummary = ({ summary }: Props) => {
    const summaryIcon = getSummaryIcon(summary);
    const summaryMessage = getSummaryMessage(summary);
    const subtitleMessage = `${summary.activeIncident} active incidents, ${summary.resolvedIncident} resolved incidents`;
    return (
        <SummaryHeader>
            <SummaryContainer>
                <div style={{ display: 'flex', alignItems: 'center' }}>
                    {summaryIcon}
                    <SummaryMessage>
                        <SummaryTitle level={5}>{summaryMessage}</SummaryTitle>
                        <Typography.Text type="secondary">{subtitleMessage}</Typography.Text>
                    </SummaryMessage>
                </div>
            </SummaryContainer>
        </SummaryHeader>
    );
};

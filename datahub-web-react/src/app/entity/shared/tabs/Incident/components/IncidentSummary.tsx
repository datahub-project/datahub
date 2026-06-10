import { CheckCircleFilled, StopOutlined, WarningFilled } from '@ant-design/icons';
import { Typography } from 'antd';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled, { DefaultTheme, useTheme } from 'styled-components';

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

type IncidentsSummary = {
    totalIncident: number;
    resolvedIncident: number;
    activeIncident: number;
};

type Props = {
    summary: IncidentsSummary;
};

const getSummaryIcon = (summary: IncidentsSummary, theme: DefaultTheme) => {
    if (summary.totalIncident === 0) {
        return <StopOutlined style={{ color: theme.colors.icon, fontSize: 28 }} />;
    }
    if (summary.resolvedIncident === summary.totalIncident) {
        return <CheckCircleFilled style={{ color: theme.colors.iconSuccess, fontSize: 28 }} />;
    }
    return <WarningFilled style={{ color: theme.colors.iconError, fontSize: 28 }} />;
};

export const IncidentSummary = ({ summary }: Props) => {
    const { t } = useTranslation('entity.profile.incident');
    const theme = useTheme();
    const summaryIcon = getSummaryIcon(summary, theme);
    let summaryMessage: string | null = null;
    if (summary.totalIncident === 0) {
        summaryMessage = t('summary.noneRaised');
    } else if (summary.resolvedIncident === summary.totalIncident) {
        summaryMessage = t('summary.noActive');
    } else if (summary.activeIncident > 0) {
        summaryMessage = t('summary.activeCount', { count: summary.activeIncident });
    }
    const subtitleMessage = t('summary.subtitle', {
        active: summary.activeIncident,
        resolved: summary.resolvedIncident,
    });
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

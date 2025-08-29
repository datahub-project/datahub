import { CheckCircle, Stop, XCircle } from '@phosphor-icons/react';
import { Tooltip, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { colors } from '@src/alchemy-components/theme';

const SummaryHeader = styled.div`
    width: 100%;
    padding-left: 20px;
    padding-top: 20px;
    padding-bottom: 20px;
    display: flex;
    align-items: center;
    border-bottom: 1px solid ${colors.gray[100]};
`;

const SummaryContainer = styled.div``;

const SummaryMessage = styled.div`
    display: inline-block;
    margin-left: 12px;
`;

const SummaryTitle = styled(Typography.Title)`
    && {
        padding-bottom: 0px;
        margin-bottom: 0px;
    }
`;

export type TestsSummary = {
    failing: number;
    passing: number;
    total: number;
};

type Props = {
    summary: TestsSummary;
};

const getSummaryIcon = (summary: TestsSummary) => {
    if (summary.total === 0) {
        return <Stop size={28} color={colors.gray[600]} />;
    }
    if (summary.passing === summary.total) {
        return <CheckCircle size={28} color={colors.green[500]} />;
    }
    return <XCircle size={28} color={colors.red[500]} />;
};

const getSummaryMessage = (summary: TestsSummary) => {
    if (summary.total === 0) {
        return 'No tests have run';
    }
    if (summary.passing === summary.total) {
        return 'All tests are passing';
    }
    if (summary.failing === summary.total) {
        return 'All tests are failing';
    }
    return 'Some tests are failing';
};

export const TestResultsSummary = ({ summary }: Props) => {
    const summaryIcon = getSummaryIcon(summary);
    const summaryMessage = getSummaryMessage(summary);
    const subtitleMessage = `${summary.passing} passing tests, ${summary.failing} failing tests`;
    return (
        <SummaryHeader>
            <SummaryContainer>
                <Tooltip title="This status is based on the most recent run of each test.">
                    <div style={{ display: 'flex', alignItems: 'center' }}>
                        {summaryIcon}
                        <SummaryMessage>
                            <SummaryTitle level={5}>{summaryMessage}</SummaryTitle>
                            <Typography.Text type="secondary">{subtitleMessage}</Typography.Text>
                        </SummaryMessage>
                    </div>
                </Tooltip>
            </SummaryContainer>
        </SummaryHeader>
    );
};

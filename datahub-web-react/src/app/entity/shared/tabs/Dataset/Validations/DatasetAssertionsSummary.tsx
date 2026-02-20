import { CheckCircleFilled, CloseCircleFilled, StopOutlined } from '@ant-design/icons';
import { Tooltip, Typography } from 'antd';
import React from 'react';
import styled, { useTheme } from 'styled-components';

const SummaryHeader = styled.div`
    width: 100%;
    padding-left: 40px;
    padding-top: 20px;
    padding-bottom: 20px;
    display: flex;
    align-items: center;
    border-bottom: 1px solid ${(props) => props.theme.colors.border};
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

export type AssertionsSummary = {
    totalAssertions: number;
    totalRuns: number;
    failedRuns: number;
    succeededRuns: number;
};

type Props = {
    summary: AssertionsSummary;
};

const getSummaryIcon = (
    summary: AssertionsSummary,
    disabledColor: string,
    successColor: string,
    errorColor: string,
) => {
    if (summary.totalRuns === 0) {
        return <StopOutlined style={{ color: disabledColor, fontSize: 28 }} />;
    }
    if (summary.succeededRuns === summary.totalRuns) {
        return <CheckCircleFilled style={{ color: successColor, fontSize: 28 }} />;
    }
    return <CloseCircleFilled style={{ color: errorColor, fontSize: 28 }} />;
};

const getSummaryMessage = (summary: AssertionsSummary) => {
    if (summary.totalRuns === 0) {
        return 'No assertions have run';
    }
    if (summary.succeededRuns === summary.totalRuns) {
        return 'All assertions have passed';
    }
    if (summary.failedRuns === summary.totalRuns) {
        return 'All assertions have failed';
    }
    return 'Some assertions have failed';
};

export const DatasetAssertionsSummary = ({ summary }: Props) => {
    const theme = useTheme();
    const summaryIcon = getSummaryIcon(
        summary,
        theme.colors.textDisabled,
        theme.colors.textSuccess,
        theme.colors.textError,
    );
    const summaryMessage = getSummaryMessage(summary);
    const subtitleMessage = `${summary.succeededRuns} successful assertions, ${summary.failedRuns} failed assertions`;
    return (
        <SummaryHeader>
            <SummaryContainer>
                <Tooltip title="This status is based on the most recent run of each assertion.">
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

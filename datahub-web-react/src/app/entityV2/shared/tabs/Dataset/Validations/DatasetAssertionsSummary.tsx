import { CheckCircleFilled, CloseCircleFilled, ExclamationCircleFilled, StopOutlined } from '@ant-design/icons';
import { Typography } from 'antd';
import { Tooltip } from '@components';
import React from 'react';
import styled from 'styled-components';
import { ANTD_GRAY } from '../../../constants';

const SummaryHeader = styled.div`
    width: 100%;
    padding-left: 40px;
    padding-top: 20px;
    padding-bottom: 20px;
    display: flex;
    align-items: center;
    border-bottom: 1px solid ${ANTD_GRAY[4.5]};
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
    erroredRuns: number;
};

type Props = {
    summary: AssertionsSummary;
};

const SUCCESS_COLOR_HEX = '#52C41A';
const FAILURE_COLOR_HEX = '#F5222D';
const ERROR_COLOR_HEX = '#FAAD14';

const getSummaryIcon = (summary: AssertionsSummary) => {
    if (summary.totalRuns === 0) {
        return <StopOutlined style={{ color: ANTD_GRAY[6], fontSize: 28 }} />;
    }
    if (summary.succeededRuns === summary.totalRuns) {
        return <CheckCircleFilled style={{ color: SUCCESS_COLOR_HEX, fontSize: 28 }} />;
    }
    if (summary.erroredRuns > 0) {
        return <ExclamationCircleFilled style={{ color: ERROR_COLOR_HEX, fontSize: 28 }} />;
    }
    return <CloseCircleFilled style={{ color: FAILURE_COLOR_HEX, fontSize: 28 }} />;
};

const getSummaryMessage = (summary: AssertionsSummary) => {
    if (summary.totalRuns === 0) {
        return 'No assertions have run';
    }
    if (summary.succeededRuns === summary.totalRuns) {
        return 'All assertions are passing';
    }
    if (summary.erroredRuns > 0) {
        return 'An error is preventing some assertions from running';
    }
    if (summary.failedRuns === summary.totalRuns) {
        return 'All assertions are failing';
    }
    return 'Some assertions are failing';
};

export const DatasetAssertionsSummary = ({ summary }: Props) => {
    const summaryIcon = getSummaryIcon(summary);
    const summaryMessage = getSummaryMessage(summary);
    const errorMessage = summary.erroredRuns
        ? `, ${summary.erroredRuns} error${summary.erroredRuns > 1 ? 's' : ''}`
        : '';
    const inactiveAssertionsCount = summary.totalAssertions - summary.totalRuns;
    const inactiveAssertionsMessage = inactiveAssertionsCount ? `, ${inactiveAssertionsCount} inactive.` : '.';
    const subtitleMessage = `${summary.succeededRuns} successful assertions, ${summary.failedRuns} failed assertions`;
    return (
        <SummaryHeader>
            <SummaryContainer>
                <Tooltip title="This status is based on the most recent result of each assertion.">
                    <div style={{ display: 'flex', alignItems: 'center' }}>
                        {summaryIcon}
                        <SummaryMessage>
                            <SummaryTitle level={5}>{summaryMessage}</SummaryTitle>
                            <Typography.Text type="secondary">
                                {subtitleMessage}
                                {errorMessage}
                                {inactiveAssertionsMessage}
                            </Typography.Text>
                        </SummaryMessage>
                    </div>
                </Tooltip>
            </SummaryContainer>
        </SummaryHeader>
    );
};

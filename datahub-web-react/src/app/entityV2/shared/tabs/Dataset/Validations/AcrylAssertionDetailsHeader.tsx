import React from 'react';
import styled from 'styled-components';
import { Divider, Typography } from 'antd';
import { Tooltip } from '@components';
import { ClockCircleOutlined } from '@ant-design/icons';
import { CronSchedule } from '../../../../../../types.generated';
import { getLocaleTimezone } from '../../../../../shared/time/timeUtils';
import { ANTD_GRAY } from '../../../constants';
import { getCronAsText } from './acrylUtils';
import { TruncatedTextWithTooltip } from '../../../../../shared/TruncatedTextWithTooltip';

const TimeLabel = styled.div`
    max-width: 280px;
    padding: 0;
    margin: 0;
    display: flex;
    align-items: center;
    color: ${ANTD_GRAY[7]};
`;

const Schedule = styled.span`
    display: flex;
    align-items: center;
    justify-content: left;
`;

const StyledTruncatedText = styled(TruncatedTextWithTooltip)`
    color: ${ANTD_GRAY[7]};
`;

const StyledClockCircleOutlined = styled(ClockCircleOutlined)`
    margin-right: 8px;
`;

type Props = {
    schedule?: CronSchedule;
    lastEvaluatedAtMillis?: number | undefined;
    nextEvaluatedAtMillis?: number | undefined;
    isStopped?: boolean;
};

/**
 * Renders the header of the Assertion Details card, which displays the run schedule for the assertion.
 */
export const AcrylAssertionDetailsHeader = ({
    schedule,
    lastEvaluatedAtMillis,
    nextEvaluatedAtMillis,
    isStopped = false,
}: Props) => {
    const localeTimezone = getLocaleTimezone();

    /**
     * Last evaluated timestamp
     */
    const lastEvaluatedAt = lastEvaluatedAtMillis && new Date(lastEvaluatedAtMillis);
    const lastEvaluatedTimeLocal = lastEvaluatedAt
        ? `Last evaluated on ${lastEvaluatedAt.toLocaleDateString()} at ${lastEvaluatedAt.toLocaleTimeString()} (${localeTimezone})`
        : null;
    const lastEvaluatedTimeGMT = lastEvaluatedAt ? lastEvaluatedAt.toUTCString() : null;

    /**
     * Next evaluated timestamp
     */
    const nextEvaluatedAt = nextEvaluatedAtMillis && new Date(nextEvaluatedAtMillis);
    const nextEvaluatedTimeLocal = nextEvaluatedAt
        ? `Next evaluation at ${nextEvaluatedAt.toLocaleDateString()} at ${nextEvaluatedAt.toLocaleTimeString()} (${localeTimezone})`
        : null;
    const nextEvaluatedTimeGMT = nextEvaluatedAt ? nextEvaluatedAt.toUTCString() : null;

    /**
     * Cron String - This will not be present for external assertions.
     */
    const interval = schedule?.cron?.replaceAll(', ', '');
    const timezone = schedule?.timezone || Intl.DateTimeFormat().resolvedOptions().timeZone;
    const cronAsText = interval && getCronAsText(interval);
    const scheduleText = schedule && cronAsText && !cronAsText.error && cronAsText.text;

    return (
        <div>
            <Typography.Title level={5}>Evaluations</Typography.Title>
            {(!isStopped && (
                <Schedule>
                    {scheduleText && (
                        <TimeLabel>
                            <StyledClockCircleOutlined />
                            <StyledTruncatedText text={`Runs ${scheduleText} (${timezone})`} maxLength={80} />
                            {lastEvaluatedTimeLocal && <Divider type="vertical" />}
                        </TimeLabel>
                    )}
                    <Tooltip placement="topLeft" title={lastEvaluatedTimeGMT}>
                        <TimeLabel>
                            {lastEvaluatedTimeLocal} {nextEvaluatedAtMillis ? <Divider type="vertical" /> : true}
                        </TimeLabel>
                    </Tooltip>
                    <Tooltip placement="topLeft" title={nextEvaluatedTimeGMT}>
                        {nextEvaluatedTimeLocal && <TimeLabel>{nextEvaluatedTimeLocal}</TimeLabel>}
                    </Tooltip>
                </Schedule>
            )) || <Typography.Text type="secondary">This assertion is not actively running.</Typography.Text>}
        </div>
    );
};

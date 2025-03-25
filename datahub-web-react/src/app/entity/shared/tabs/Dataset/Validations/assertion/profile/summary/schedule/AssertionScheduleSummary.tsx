import React from 'react';

import styled from 'styled-components';
import { ClockCircleOutlined, StopOutlined } from '@ant-design/icons';
import { extractLatestGeneratedAt } from '@src/app/entityV2/shared/tabs/Dataset/Validations/acrylUtils';

import { Assertion, AssertionSourceType, CronSchedule, Monitor } from '../../../../../../../../../../types.generated';
import { getLocaleTimezone } from '../../../../../../../../../shared/time/timeUtils';
import { getCronAsText } from '../../../../acrylUtils';
import { AssertionScheduleSummarySection } from './AssertionScheduleSummarySection';
import { isExternalAssertion } from '../../shared/isExternalAssertion';
import { ProviderSummarySection } from './ProviderSummarySection';
import { InferredAssertionLogo } from '../../../../InferredAssertionLogo';

const Container = styled.div`
    margin-top: 20px;
`;

const Sections = styled.div`
    margin: 20px 4px;
`;

const StyledClockCircleOutlined = styled(ClockCircleOutlined)`
    margin-right: 8px;
    font-size: 14px;
`;

const StyledStopOutlined = styled(StopOutlined)`
    margin-right: 8px;
    font-size: 14px;
`;

const StyledLastUpdatedLogo = styled(InferredAssertionLogo)`
    margin-right: 8px;
`;

type Props = {
    assertion: Assertion;
    monitor?: Monitor;
    schedule?: CronSchedule;
    lastEvaluatedAtMillis?: number | undefined;
    nextEvaluatedAtMillis?: number | undefined;
    isStopped?: boolean;
};

/**
 * Renders the header of the Assertion Details card, which displays the run schedule for the assertion.
 */
export const AssertionScheduleSummary = ({
    assertion,
    monitor,
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

    /**
     * For external assertions, show the running platforms
     */
    const isExternal = isExternalAssertion(assertion);

    /**
     * For smart assertions, show the last time the rule was updated.
     */
    const isSmartAssertion = assertion?.info?.source?.type === AssertionSourceType.Inferred;
    const generatedAt = isSmartAssertion && monitor ? new Date(extractLatestGeneratedAt(monitor) || 0) : undefined;
    const lastUpdatedAtTimeLocal = generatedAt
        ? `${generatedAt.toLocaleDateString()} at ${generatedAt.toLocaleTimeString()} (${localeTimezone})`
        : null;
    const lastUpdatedAtTimeGmt = generatedAt ? generatedAt.toUTCString() : null;

    return (
        <Container>
            <Sections>
                {scheduleText && (
                    <AssertionScheduleSummarySection
                        icon={<StyledClockCircleOutlined />}
                        title="Run schedule"
                        subtitle={`Runs ${scheduleText} (${timezone})`}
                        showDivider
                    />
                )}
                <AssertionScheduleSummarySection
                    icon={<StyledClockCircleOutlined />}
                    title="Last evaluated"
                    subtitle={
                        (lastEvaluatedTimeLocal && lastEvaluatedTimeLocal) ||
                        'This assertion has not been evaluated yet.'
                    }
                    tooltip={lastEvaluatedTimeGMT}
                    showDivider
                />
                {(isExternal && assertion.platform && <ProviderSummarySection assertion={assertion} />) || null}
                {(nextEvaluatedTimeLocal && !isStopped && (
                    <AssertionScheduleSummarySection
                        icon={<StyledClockCircleOutlined />}
                        title="Next evaluation"
                        subtitle={nextEvaluatedTimeLocal}
                        tooltip={nextEvaluatedTimeGMT}
                        showDivider={!!lastUpdatedAtTimeLocal}
                    />
                )) ||
                    (nextEvaluatedTimeLocal && (
                        <AssertionScheduleSummarySection
                            icon={<StyledStopOutlined />}
                            title="Next evaluation"
                            subtitle="This assertion is not actively running. Start the assertion to view the next evaluation time."
                            showDivider={!!lastUpdatedAtTimeLocal}
                        />
                    )) ||
                    null}
                {lastUpdatedAtTimeLocal ? (
                    <AssertionScheduleSummarySection
                        icon={<StyledLastUpdatedLogo />}
                        title="Last trained"
                        subtitle={`Monitor predictions were last refreshed at ${lastUpdatedAtTimeLocal}.`}
                        tooltip={lastUpdatedAtTimeGmt}
                        showDivider={false}
                    />
                ) : null}
            </Sections>
        </Container>
    );
};

import { ClockCircleOutlined, StopOutlined } from '@ant-design/icons';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { getCronAsText } from '@app/entityV2/shared/tabs/Dataset/Validations/acrylUtils';
import { isExternalAssertion } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/shared/isExternalAssertion';
import { AssertionScheduleSummarySection } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/schedule/AssertionScheduleSummarySection';
import { ProviderSummarySection } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/schedule/ProviderSummarySection';
import { getLocaleTimezone } from '@app/shared/time/timeUtils';

import { Assertion, CronSchedule } from '@types';

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

type Props = {
    assertion: Assertion;
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
    schedule,
    lastEvaluatedAtMillis,
    nextEvaluatedAtMillis,
    isStopped = false,
}: Props) => {
    const { t } = useTranslation('entity.profile.validations');
    const localeTimezone = getLocaleTimezone();

    /**
     * Last evaluated timestamp
     */
    const lastEvaluatedAt = lastEvaluatedAtMillis && new Date(lastEvaluatedAtMillis);
    const lastEvaluatedTimeLocal = lastEvaluatedAt
        ? t('schedule.lastEvaluatedOn', {
              date: lastEvaluatedAt.toLocaleDateString(),
              time: lastEvaluatedAt.toLocaleTimeString(),
              timezone: localeTimezone,
          })
        : null;
    const lastEvaluatedTimeGMT = lastEvaluatedAt ? lastEvaluatedAt.toUTCString() : null;

    /**
     * Next evaluated timestamp
     */
    const nextEvaluatedAt = nextEvaluatedAtMillis && new Date(nextEvaluatedAtMillis);
    const nextEvaluatedTimeLocal = nextEvaluatedAt
        ? t('schedule.nextEvaluationAt', {
              date: nextEvaluatedAt.toLocaleDateString(),
              time: nextEvaluatedAt.toLocaleTimeString(),
              timezone: localeTimezone,
          })
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

    return (
        <Container>
            <Sections>
                {scheduleText && (
                    <AssertionScheduleSummarySection
                        icon={<StyledClockCircleOutlined />}
                        title={t('schedule.runSchedule')}
                        subtitle={t('schedule.runs', { scheduleText, timezone })}
                        showDivider
                    />
                )}
                <AssertionScheduleSummarySection
                    icon={<StyledClockCircleOutlined />}
                    title={t('schedule.lastEvaluated')}
                    subtitle={(lastEvaluatedTimeLocal && lastEvaluatedTimeLocal) || t('schedule.notEvaluatedYet')}
                    tooltip={lastEvaluatedTimeGMT}
                    showDivider
                />
                {(isExternal && assertion.platform && <ProviderSummarySection assertion={assertion} />) || null}
                {(nextEvaluatedTimeLocal && !isStopped && (
                    <AssertionScheduleSummarySection
                        icon={<StyledClockCircleOutlined />}
                        title={t('schedule.nextEvaluation')}
                        subtitle={nextEvaluatedTimeLocal}
                        tooltip={nextEvaluatedTimeGMT}
                        showDivider={false}
                    />
                )) ||
                    (nextEvaluatedTimeLocal && (
                        <AssertionScheduleSummarySection
                            icon={<StyledStopOutlined />}
                            title={t('schedule.nextEvaluation')}
                            subtitle={t('schedule.notActivelyRunning')}
                            showDivider={false}
                        />
                    )) ||
                    null}
            </Sections>
        </Container>
    );
};

import { ClockCircleOutlined } from '@ant-design/icons';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { isExternalAssertion } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/shared/isExternalAssertion';
import { AssertionScheduleSummarySection } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/schedule/AssertionScheduleSummarySection';
import { ProviderSummarySection } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/schedule/ProviderSummarySection';
import { getLocaleTimezone } from '@app/shared/time/timeUtils';

import { Assertion } from '@types';

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

type Props = {
    assertion: Assertion;
    lastEvaluatedAtMillis?: number | undefined;
};

/**
 * Renders the schedule details of the Assertion Details card. In OSS this shows the last evaluated
 * time (derived from run events) and, for external assertions, the platform that runs the assertion.
 */
export const AssertionScheduleSummary = ({ assertion, lastEvaluatedAtMillis }: Props) => {
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
     * For external assertions, show the running platform
     */
    const isExternal = isExternalAssertion(assertion);
    const showProvider = isExternal && !!assertion.platform;

    return (
        <Container>
            <Sections>
                <AssertionScheduleSummarySection
                    icon={<StyledClockCircleOutlined />}
                    title={t('schedule.lastEvaluated')}
                    subtitle={(lastEvaluatedTimeLocal && lastEvaluatedTimeLocal) || t('schedule.notEvaluatedYet')}
                    tooltip={lastEvaluatedTimeGMT}
                    showDivider={showProvider}
                />
                {(showProvider && <ProviderSummarySection assertion={assertion} showDivider={false} />) || null}
            </Sections>
        </Container>
    );
};

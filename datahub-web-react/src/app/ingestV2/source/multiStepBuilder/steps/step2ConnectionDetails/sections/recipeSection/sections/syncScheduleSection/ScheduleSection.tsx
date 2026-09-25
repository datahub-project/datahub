import React, { useEffect, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import analytics, { EventType } from '@app/analytics';
import { SourceBuilderState } from '@app/ingestV2/source/builder/types';
import { SectionName } from '@app/ingestV2/source/multiStepBuilder/components/SectionName';
import { ScheduleFields } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/sections/syncScheduleSection/ScheduleFields';
import { DAILY_MIDNIGHT_CRON_INTERVAL } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/sections/syncScheduleSection/constants';
import { useScheduleStepSubtitle } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/sections/syncScheduleSection/useScheduleStepSubtitle';
import { IngestionSourceFormStep, MultiStepSourceBuilderState } from '@app/ingestV2/source/multiStepBuilder/types';
import { useMultiStepContext } from '@app/sharedV2/forms/multiStepForm/MultiStepFormContext';

const SectionContainer = styled.div`
    display: flex;
    flex-direction: column;
    gap: 16px;
`;

export function ScheduleSection() {
    const { t } = useTranslation('ingestion.sourceBuilder');
    const { updateState, state } = useMultiStepContext<MultiStepSourceBuilderState, IngestionSourceFormStep>();
    const { schedule } = state;
    const interval = schedule?.interval?.replaceAll(', ', ' ') || DAILY_MIDNIGHT_CRON_INTERVAL;
    const timezone = schedule?.timezone || Intl.DateTimeFormat().resolvedOptions().timeZone;
    const [scheduleEnabled, setScheduleEnabled] = useState(!!schedule);
    const [scheduleCronInterval, setScheduleCronInterval] = useState(interval);
    const [scheduleTimezone, setScheduleTimezone] = useState(timezone);

    const subtitle = useScheduleStepSubtitle();

    const analyticsRef = useRef(false);

    useEffect(() => {
        if (scheduleEnabled) {
            const newState: SourceBuilderState = {
                ...state,
                schedule: {
                    timezone: scheduleTimezone,
                    interval: scheduleCronInterval,
                },
            };
            updateState(newState);
        } else {
            const newState: SourceBuilderState = {
                ...state,
                schedule: null,
            };
            updateState(newState);
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [scheduleCronInterval, scheduleEnabled, scheduleTimezone]);

    useEffect(() => {
        if (analyticsRef.current) return;
        if (state) {
            analyticsRef.current = true;
            analytics.event({
                type: EventType.IngestionEnterSyncScheduleEvent,
                sourceType: state.type || '',
                sourceUrn: state.ingestionSource?.urn,
                configurationType: state.isEditing ? 'edit_existing' : 'create_new',
            });
        }
    }, [state]);

    return (
        <SectionContainer data-testid="sync-schedule-section">
            <SectionName name={t('multiStep.schedule.title')} description={subtitle} />
            <ScheduleFields
                scheduleEnabled={scheduleEnabled}
                onScheduleEnabledChange={setScheduleEnabled}
                cronInterval={scheduleCronInterval}
                onCronIntervalChange={setScheduleCronInterval}
                timezone={scheduleTimezone}
                onTimezoneChange={setScheduleTimezone}
            />
        </SectionContainer>
    );
}

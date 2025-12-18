import { Icon, Switch, Text } from '@components';
import cronstrue from 'cronstrue';
import React, { useEffect, useMemo, useState } from 'react';
import styled from 'styled-components';

import { TimezoneSelect } from '@app/ingestV2/source/builder/TimezoneSelect';
import { SourceBuilderState } from '@app/ingestV2/source/builder/types';
import CronField from '@app/ingestV2/source/multiStepBuilder/steps/step3SyncSchedule/CronField';
import { DAILY_MIDNIGHT_CRON_INTERVAL } from '@app/ingestV2/source/multiStepBuilder/steps/step3SyncSchedule/constants';
import { IngestionSourceFormStep, MultiStepSourceBuilderState } from '@app/ingestV2/source/multiStepBuilder/types';
import { lowerFirstLetter } from '@app/shared/textUtil';
import { useMultiStepContext } from '@app/sharedV2/forms/multiStepForm/MultiStepFormContext';

const StepContainer = styled.div`
    display: flex;
    flex-direction: column;
    gap: 16px;
`;

const SwitchLabel = styled.div`
    display: flex;
    gap: 2px;
`;

const WarningContainer = styled.div`
    display: flex;
    align-items: center;
    gap: 4px;
`;

const TimezoneContainer = styled.div`
    display: flex;
    flex-direction: column;
    gap: 8px;
`;

export function ScheduleStep() {
    const { updateState, setCurrentStepCompleted, setCurrentStepUncompleted, state } = useMultiStepContext<
        MultiStepSourceBuilderState,
        IngestionSourceFormStep
    >();
    const { schedule } = state;
    const interval = schedule?.interval?.replaceAll(', ', ' ') || DAILY_MIDNIGHT_CRON_INTERVAL;
    const timezone = schedule?.timezone || Intl.DateTimeFormat().resolvedOptions().timeZone;
    const [scheduleEnabled, setScheduleEnabled] = useState(!!schedule);
    const [scheduleCronInterval, setScheduleCronInterval] = useState(interval);
    const [scheduleTimezone, setScheduleTimezone] = useState(timezone);

    const cronAsText = useMemo(() => {
        if (scheduleCronInterval) {
            try {
                return {
                    text: `Runs ${lowerFirstLetter(cronstrue.toString(scheduleCronInterval))}.`,
                    error: false,
                };
            } catch (e) {
                return {
                    text: undefined,
                    error: true,
                };
            }
        }
        return {
            text: undefined,
            error: false,
        };
    }, [scheduleCronInterval]);

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
                schedule: undefined,
            };
            updateState(newState);
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [scheduleCronInterval, scheduleEnabled, scheduleTimezone]);

    useEffect(() => {
        if (cronAsText.error) {
            setCurrentStepUncompleted();
        } else {
            setCurrentStepCompleted();
        }
    }, [cronAsText, setCurrentStepCompleted, setCurrentStepUncompleted]);

    return (
        <StepContainer>
            <SwitchLabel>
                <Text size="sm" weight="bold" color="gray" colorLevel={600}>
                    Run on a schedule
                </Text>
                <Text size="sm" weight="bold" color="gray" colorLevel={1700}>
                    (recommended)
                </Text>
            </SwitchLabel>
            <Switch
                label="Enable to run ingestion syncs on a schedule. Running syncs on a schedule helps to keep information up to date."
                checked={scheduleEnabled}
                onChange={(e) => setScheduleEnabled(e.target.checked)}
                labelPosition="right"
            />
            {!scheduleEnabled && (
                <WarningContainer>
                    <Icon icon="Warning" source="phosphor" color="yellow" colorLevel={1000} size="md" />
                    <Text color="yellow" colorLevel={1000} size="sm">
                        Running ingestion without a schedule may result in out-of-date information.
                    </Text>
                </WarningContainer>
            )}
            <CronField
                scheduleCronInterval={scheduleCronInterval}
                setScheduleCronInterval={setScheduleCronInterval}
                cronAsText={cronAsText}
            />
            <TimezoneContainer>
                <Text color="gray">Choose a timezone for the schedule.</Text>
                <TimezoneSelect value={scheduleTimezone} onChange={setScheduleTimezone} />
            </TimezoneContainer>
        </StepContainer>
    );
}

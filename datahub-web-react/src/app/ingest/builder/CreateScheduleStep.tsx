import { Button, Input, Typography } from 'antd';
import React, { useMemo } from 'react';
import styled from 'styled-components';
import cronstrue from 'cronstrue';
import { BaseBuilderState, IngestionSourceBuilderStep, StepProps } from './types';
import { TimezoneSelect } from './TimezoneSelect';
import { ANTD_GRAY } from '../../entity/shared/constants';

const Section = styled.div`
    display: flex;
    flex-direction: column;
    max-width: 400px;
    padding-bottom: 12px;
    padding-top: 12px;
`;

const SelectTemplateHeader = styled(Typography.Title)`
    && {
        margin-bottom: 16px;
    }
`;

const CronText = styled(Typography.Text)`
    color: ${ANTD_GRAY[9]};
`;

export const CreateScheduleStep = ({ state, updateState, goTo, prev }: StepProps) => {
    const interval = state.schedule?.interval || '';
    const timezone = state.schedule?.timezone || Intl.DateTimeFormat().resolvedOptions().timeZone;

    const setTimezone = (tz: string) => {
        const newState: BaseBuilderState = {
            ...state,
            schedule: {
                ...state.schedule,
                timezone: tz,
            },
        };
        updateState(newState);
    };

    const setCronInterval = (int: string) => {
        const newState: BaseBuilderState = {
            ...state,
            schedule: {
                ...state.schedule,
                interval: int,
            },
        };
        updateState(newState);
    };

    const cronAsText = useMemo(() => {
        if (interval) {
            try {
                return `Runs ${cronstrue.toString(interval).toLowerCase()}`;
            } catch (e) {
                return "Invalid cron schedule. Cron must be of UNIX form: 'minute, hour, day, month, dayofweek'";
            }
        }
        return undefined;
    }, [interval]);

    const onClickNext = () => {
        goTo(IngestionSourceBuilderStep.NAME_SOURCE);
    };

    const onClickSkip = () => {
        goTo(IngestionSourceBuilderStep.NAME_SOURCE);
    };

    return (
        <>
            <Section>
                <SelectTemplateHeader level={5}>Create a Schedule</SelectTemplateHeader>
                <Typography.Text>Run this ingestion source on a schedule.</Typography.Text>
            </Section>
            <Typography.Text strong>Timezone</Typography.Text>
            <Section>
                <Typography.Text>Select the timezone to run the schedule in.</Typography.Text>
                <TimezoneSelect value={timezone} onChange={setTimezone} />
            </Section>
            <Typography.Text strong>Schedule</Typography.Text>
            <Section>
                <Typography.Text>Provide a custom cron schedule.</Typography.Text>
                <Input value={interval} onChange={(e) => setCronInterval(e.target.value)} placeholder="* * * * *" />
            </Section>
            <Section>{cronAsText && <CronText>{cronAsText}</CronText>}</Section>
            <div style={{ display: 'flex', justifyContent: 'space-between', marginTop: 8 }}>
                <Button onClick={prev}>Previous</Button>
                <div>
                    <Button style={{ marginRight: 8 }} onClick={onClickSkip}>
                        Skip
                    </Button>
                    <Button disabled={!(interval !== undefined)} onClick={onClickNext}>
                        Next
                    </Button>
                </div>
            </div>
        </>
    );
};

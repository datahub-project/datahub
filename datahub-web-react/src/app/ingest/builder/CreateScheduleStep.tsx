import { Button, DatePicker, Typography } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';
import { CronBuilder } from './CronBuilder';
import { CustomCronBuilder } from './CustomCronBuilder';
import { IngestionSourceBuilderStep, RecipeBuilderState, StepProps } from './types';

const Section = styled.div`
    display: flex;
    flex-direction: column;
    padding-bottom: 12px;
`;

const SelectTemplateHeader = styled(Typography.Title)`
    && {
        margin-bottom: 16px;
    }
`;

enum ViewType {
    CRON_BUILDER,
    CUSTOM_CRON,
}

export const CreateScheduleStep = ({ state, updateState, goTo, prev }: StepProps) => {
    // Cron interval is assumed to be defined in UTC time, not local time.
    const [cronInterval, setCronInterval] = useState<undefined | string>(undefined);

    // The time at which the ingestion source should begin to be executed.
    const [startTimeMs, setStartTimeMs] = useState<undefined | number>(undefined);

    // The cron builder experience to show
    const [viewType, setViewType] = useState<ViewType>(ViewType.CRON_BUILDER);

    const onClickNext = () => {
        if (cronInterval !== undefined && startTimeMs !== undefined) {
            const oldState = state as RecipeBuilderState;
            const newState: RecipeBuilderState = {
                ...oldState,
                schedule: {
                    interval: cronInterval,
                    startTimeMs,
                },
            };
            updateState(newState);
            goTo(IngestionSourceBuilderStep.NAME_SOURCE);
        }
    };

    const onClickSkip = () => {
        goTo(IngestionSourceBuilderStep.NAME_SOURCE);
    };

    const onSelectStartTime = (value) => {
        const newStartTimeMs = value.valueOf();
        setStartTimeMs(newStartTimeMs);
    };

    return (
        <>
            <Section>
                <SelectTemplateHeader level={5}>Create a Schedule</SelectTemplateHeader>
                <Typography.Text>Run this ingestion source on a schedule.</Typography.Text>
            </Section>
            <Section>
                {(viewType === ViewType.CRON_BUILDER && (
                    <>
                        <CronBuilder updateCron={(newCron) => setCronInterval(newCron)} />
                        <Typography.Text>
                            Or provide a{' '}
                            <Button type="link" onClick={() => setViewType(ViewType.CUSTOM_CRON)}>
                                custom cron expression.
                            </Button>
                        </Typography.Text>
                    </>
                )) || <CustomCronBuilder updateCron={(newCron) => setCronInterval(newCron)} />}
                <Typography.Text>Starting on</Typography.Text>
                <DatePicker onChange={onSelectStartTime} />
            </Section>
            <div style={{ display: 'flex', justifyContent: 'space-between', marginTop: 8 }}>
                <Button onClick={prev}>Previous</Button>
                <div>
                    <Button onClick={onClickSkip}>Skip</Button>
                    <Button disabled={!(cronInterval !== undefined && startTimeMs !== undefined)} onClick={onClickNext}>
                        Next
                    </Button>
                </div>
            </div>
        </>
    );
};

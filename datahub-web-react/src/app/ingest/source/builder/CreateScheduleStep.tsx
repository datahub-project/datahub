import { Button, Form, Input, Typography } from 'antd';
import React, { useMemo } from 'react';
import styled from 'styled-components';
import cronstrue from 'cronstrue';
import { CheckCircleOutlined } from '@ant-design/icons';
import { SourceBuilderState, StepProps } from './types';
import { TimezoneSelect } from './TimezoneSelect';
import { ANTD_GRAY, REDESIGN_COLORS } from '../../../entity/shared/constants';
import { lowerFirstLetter } from '../../../shared/textUtil';
import { IngestionSourceBuilderStep } from './steps';

const Section = styled.div`
    display: flex;
    flex-direction: column;
    padding-bottom: 16px;
    padding-top: 0px;
`;

const SelectTemplateHeader = styled(Typography.Title)`
    && {
        margin-bottom: 8px;
    }
`;

const CronText = styled(Typography.Paragraph)`
    &&& {
        margin-top: 8px;
        margin-bottom: 0px;
    }
    color: ${ANTD_GRAY[7]};
`;

const CronSuccessCheck = styled(CheckCircleOutlined)`
    color: ${REDESIGN_COLORS.BLUE};
    margin-right: 4px;
`;

const ControlsContainer = styled.div`
    display: flex;
    justify-content: space-between;
    margin-top: 8px;
`;

const ItemDescriptionText = styled(Typography.Paragraph)``;

export const CreateScheduleStep = ({ state, updateState, goTo, prev }: StepProps) => {
    const interval = state.schedule?.interval || '';
    const timezone = state.schedule?.timezone || Intl.DateTimeFormat().resolvedOptions().timeZone;

    const setTimezone = (tz: string) => {
        const newState: SourceBuilderState = {
            ...state,
            schedule: {
                ...state.schedule,
                timezone: tz,
            },
        };
        updateState(newState);
    };

    const setCronInterval = (int: string) => {
        const newState: SourceBuilderState = {
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
                return {
                    text: `Runs ${lowerFirstLetter(cronstrue.toString(interval))}.`,
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
    }, [interval]);

    const onClickNext = () => {
        setTimezone(timezone);
        goTo(IngestionSourceBuilderStep.NAME_SOURCE);
    };

    const onClickSkip = () => {
        const newState: SourceBuilderState = {
            ...state,
            schedule: undefined,
        };
        updateState(newState);
        goTo(IngestionSourceBuilderStep.NAME_SOURCE);
    };

    return (
        <>
            <Section>
                <SelectTemplateHeader level={5}>Create an Execution Schedule</SelectTemplateHeader>
                <Typography.Text>Configure your ingestion source to run on a schedule.</Typography.Text>
            </Section>
            <Form layout="vertical">
                <Form.Item required label={<Typography.Text strong>Schedule</Typography.Text>}>
                    <ItemDescriptionText>Provide a custom cron schedule.</ItemDescriptionText>
                    <Input value={interval} onChange={(e) => setCronInterval(e.target.value)} placeholder="* * * * *" />
                    <CronText>
                        {cronAsText.error && <>Invalid cron schedule. Cron must be of UNIX form:</>}
                        {!cronAsText.text && (
                            <Typography.Paragraph keyboard style={{ marginTop: 4 }}>
                                minute, hour, day, month, day of week
                            </Typography.Paragraph>
                        )}
                        {cronAsText.text && (
                            <>
                                <CronSuccessCheck />
                                {cronAsText.text}
                            </>
                        )}
                    </CronText>
                </Form.Item>
                <Form.Item required label={<Typography.Text strong>Timezone</Typography.Text>}>
                    <ItemDescriptionText>Select the timezone to run the cron schedule in.</ItemDescriptionText>
                    <TimezoneSelect value={timezone} onChange={setTimezone} />
                </Form.Item>
            </Form>
            <ControlsContainer>
                <Button onClick={prev}>Previous</Button>
                <div>
                    <Button style={{ marginRight: 8 }} onClick={onClickSkip}>
                        Skip
                    </Button>
                    <Button disabled={!interval || interval.length === 0 || cronAsText.error} onClick={onClickNext}>
                        Next
                    </Button>
                </div>
            </ControlsContainer>
        </>
    );
};

import { CheckCircleOutlined, WarningOutlined } from '@ant-design/icons';
import { Checkbox, Form, Input, Switch, Typography } from 'antd';
import React, { useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Cron } from 'react-js-cron';
import 'react-js-cron/dist/styles.css';
import styled from 'styled-components';

import { TimezoneSelect } from '@app/ingestV2/source/builder/TimezoneSelect';
import { IngestionSourceBuilderStep } from '@app/ingestV2/source/builder/steps';
import { SourceBuilderState, StepProps } from '@app/ingestV2/source/builder/types';
import { RequiredFieldForm } from '@app/shared/form/RequiredFieldForm';
import { lowerFirstLetter } from '@app/shared/textUtil';
import { Button } from '@src/alchemy-components';
import { cronToString } from '@utils/cronstrue';

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
        margin-bottom: 0px;
    }
    color: ${(props) => props.theme.colors.textTertiary};
`;

const CronInput = styled(Input)`
    margin-bottom: 8px;
    max-width: 200px;
`;

const Schedule = styled.div`
    display: flex;
    align-items: center;
    justify-content: start;
`;

const AdvancedSchedule = styled.div`
    margin-left: 20px;
`;

const AdvancedCheckBox = styled(Typography.Text)`
    margin-right: 10px;
`;

const CronSuccessCheck = styled(CheckCircleOutlined)`
    color: ${(props) => props.theme.colors.textBrand};
    margin-right: 4px;
`;

const ControlsContainer = styled.div`
    display: flex;
    justify-content: space-between;
    margin-top: 8px;
`;

const StyledFormItem = styled(Form.Item)`
    .cron-builder {
        color: ${(props) => props.theme.colors.textTertiary};
    }
    .cron-builder-select {
        min-width: 100px;
    }
`;

const WarningContainer = styled.div`
    color: ${(props) => props.theme.colors.textTertiary};
`;

const StyledWarningOutlined = styled(WarningOutlined)`
    margin-right: 4px;
    margin-top: 12px;
`;

const ItemDescriptionText = styled(Typography.Paragraph)``;

const DAILY_MIDNIGHT_CRON_INTERVAL = '0 0 * * *';

export const CreateScheduleStep = ({ state, updateState, goTo, prev }: StepProps) => {
    const { t } = useTranslation('ingestion.sourceBuilder');
    const { t: tc } = useTranslation('common.actions');
    const { schedule } = state;
    const interval = schedule?.interval?.replaceAll(', ', ' ') || DAILY_MIDNIGHT_CRON_INTERVAL;
    const timezone = schedule?.timezone || Intl.DateTimeFormat().resolvedOptions().timeZone;
    const [scheduleEnabled, setScheduleEnabled] = useState(!!schedule);
    const [advancedCronCheck, setAdvancedCronCheck] = useState(false);
    const [scheduleCronInterval, setScheduleCronInterval] = useState(interval);
    const [scheduleTimezone, setScheduleTimezone] = useState(timezone);

    const cronAsText = useMemo(() => {
        if (scheduleCronInterval) {
            try {
                return {
                    text: t('schedule.runs', { schedule: lowerFirstLetter(cronToString(scheduleCronInterval)) }),
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
    }, [scheduleCronInterval, t]);

    const onClickNext = () => {
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

        goTo(IngestionSourceBuilderStep.NAME_SOURCE);
    };

    return (
        <>
            <Section>
                <SelectTemplateHeader level={5} data-testid="configure-schedule-heading">
                    {t('schedule.title')}
                </SelectTemplateHeader>
            </Section>
            <RequiredFieldForm layout="vertical">
                <Form.Item
                    tooltip={t('schedule.enableTooltip')}
                    label={
                        <Typography.Text strong>
                            {t('schedule.runOnSchedule')}{' '}
                            <Typography.Text type="secondary">{t('schedule.recommended')}</Typography.Text>
                        </Typography.Text>
                    }
                >
                    <Switch checked={scheduleEnabled} onChange={(v) => setScheduleEnabled(v)} />
                    {!scheduleEnabled && (
                        <WarningContainer>
                            <StyledWarningOutlined />
                            {t('schedule.noScheduleWarning')}
                        </WarningContainer>
                    )}
                </Form.Item>
                <StyledFormItem
                    required
                    label={<Typography.Text strong>{t('schedule.scheduleLabel')}</Typography.Text>}
                >
                    <Schedule>
                        {advancedCronCheck ? (
                            <CronInput
                                placeholder={DAILY_MIDNIGHT_CRON_INTERVAL}
                                autoFocus
                                value={scheduleCronInterval}
                                onChange={(e) => setScheduleCronInterval(e.target.value)}
                            />
                        ) : (
                            <Cron
                                value={scheduleCronInterval}
                                setValue={setScheduleCronInterval}
                                clearButton={false}
                                className="cron-builder"
                                leadingZero
                            />
                        )}
                        <AdvancedSchedule>
                            <AdvancedCheckBox type="secondary">{t('schedule.showAdvanced')}</AdvancedCheckBox>
                            <Checkbox
                                checked={advancedCronCheck}
                                onChange={(event) => setAdvancedCronCheck(event.target.checked)}
                            />
                        </AdvancedSchedule>
                    </Schedule>
                    <CronText>
                        {cronAsText.error && <>{t('schedule.invalidCron')}</>}
                        {!cronAsText.text && (
                            <Typography.Paragraph keyboard style={{ marginTop: 4 }}>
                                {t('schedule.cronFormat')}
                            </Typography.Paragraph>
                        )}
                        {cronAsText.text && (
                            <>
                                <CronSuccessCheck />
                                {cronAsText.text}
                            </>
                        )}
                    </CronText>
                </StyledFormItem>
                <Form.Item required label={<Typography.Text strong>{t('schedule.timezoneLabel')}</Typography.Text>}>
                    <ItemDescriptionText>{t('schedule.timezoneDescription')}</ItemDescriptionText>
                    <TimezoneSelect value={scheduleTimezone} onChange={setScheduleTimezone} />
                </Form.Item>
            </RequiredFieldForm>
            <ControlsContainer>
                <Button variant="outline" color="gray" onClick={prev}>
                    {tc('previous')}
                </Button>
                <div>
                    <Button
                        data-testid="ingestion-schedule-next-button"
                        disabled={!interval || interval.length === 0 || cronAsText.error}
                        onClick={onClickNext}
                    >
                        {tc('next')}
                    </Button>
                </div>
            </ControlsContainer>
        </>
    );
};

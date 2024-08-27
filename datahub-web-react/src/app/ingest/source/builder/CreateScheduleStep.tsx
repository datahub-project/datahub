import { Button, Checkbox, Form, Input, Switch, Typography } from 'antd';
import React, { useMemo, useState } from 'react';
import { Cron } from 'react-js-cron';
import 'react-js-cron/dist/styles.css';
import styled from 'styled-components';
import cronstrue from 'cronstrue';
import { CheckCircleOutlined, WarningOutlined } from '@ant-design/icons';
import { useTranslation } from 'react-i18next';
import { SourceBuilderState, StepProps } from './types';
import { TimezoneSelect } from './TimezoneSelect';
import { ANTD_GRAY, REDESIGN_COLORS } from '../../../entity/shared/constants';
import { lowerFirstLetter } from '../../../shared/textUtil';
import { IngestionSourceBuilderStep } from './steps';
import { RequiredFieldForm } from '../../../shared/form/RequiredFieldForm';

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
    color: ${ANTD_GRAY[7]};
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
    color: ${REDESIGN_COLORS.BLUE};
    margin-right: 4px;
`;

const ControlsContainer = styled.div`
    display: flex;
    justify-content: space-between;
    margin-top: 8px;
`;

const StyledFormItem = styled(Form.Item)`
    .cron-builder {
        color: ${ANTD_GRAY[7]};
    }
    .cron-builder-select {
        min-width: 100px;
    }
`;

const WarningContainer = styled.div`
    color: ${ANTD_GRAY[7]};
`;

const StyledWarningOutlined = styled(WarningOutlined)`
    margin-right: 4px;
    margin-top: 12px;
`;

const ItemDescriptionText = styled(Typography.Paragraph)``;

const DAILY_MIDNIGHT_CRON_INTERVAL = '0 0 * * *';

export const CreateScheduleStep = ({ state, updateState, goTo, prev }: StepProps) => {
    const { t } = useTranslation();
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
                    text: `${t('common.runs')} ${lowerFirstLetter(cronstrue.toString(scheduleCronInterval))}.`,
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
                <SelectTemplateHeader level={5}>{t('ingest.configureAnIngestionSchedule')}</SelectTemplateHeader>
            </Section>
            <RequiredFieldForm layout="vertical">
                <Form.Item
                    tooltip={t('ingest.runAScheduleToolTip')}
                    label={
                        <Typography.Text strong>
                            {t('ingest.runOnSchedule')}{' '}
                            <Typography.Text type="secondary">{t('common.recommended')}</Typography.Text>
                        </Typography.Text>
                    }
                >
                    <Switch checked={scheduleEnabled} onChange={(v) => setScheduleEnabled(v)} />
                    {!scheduleEnabled && (
                        <WarningContainer>
                            <StyledWarningOutlined />
                            {t('ingest.runIngestionWithoutASchedule')}
                        </WarningContainer>
                    )}
                </Form.Item>
                <StyledFormItem required label={<Typography.Text strong>{t('common.schedule')}</Typography.Text>}>
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
                            <AdvancedCheckBox type="secondary">{t('common.advanced')}</AdvancedCheckBox>
                            <Checkbox
                                checked={advancedCronCheck}
                                onChange={(event) => setAdvancedCronCheck(event.target.checked)}
                            />
                        </AdvancedSchedule>
                    </Schedule>
                    <CronText>
                        {cronAsText.error && <>Invalid cron schedule. Cron must be of UNIX form:</>}
                        {!cronAsText.text && (
                            <Typography.Paragraph keyboard style={{ marginTop: 4 }}>
                                {t('common.minHourDayMouthDayOfWeek')}
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
                <Form.Item required label={<Typography.Text strong>{t('common.timezone')}</Typography.Text>}>
                    <ItemDescriptionText>{t('ingest.chooseATimezoneForTheSchedule')}</ItemDescriptionText>
                    <TimezoneSelect value={scheduleTimezone} onChange={setScheduleTimezone} />
                </Form.Item>
            </RequiredFieldForm>
            <ControlsContainer>
                <Button onClick={prev}>{t('common.previous')}</Button>
                <div>
                    <Button
                        data-testid="ingestion-schedule-next-button"
                        disabled={!interval || interval.length === 0 || cronAsText.error}
                        onClick={onClickNext}
                        type="primary"
                    >
                        {t('common.next')}
                    </Button>
                </div>
            </ControlsContainer>
        </>
    );
};

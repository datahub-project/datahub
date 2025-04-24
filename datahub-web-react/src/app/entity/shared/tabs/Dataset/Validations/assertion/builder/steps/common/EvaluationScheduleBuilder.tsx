import { ClockCircleOutlined, InfoCircleOutlined } from '@ant-design/icons';
import { Tooltip } from '@components';
import { Typography } from 'antd';
import cronstrue from 'cronstrue';
import React, { useMemo, useRef } from 'react';
import { Cron, PeriodType, SetValueFunctionExtra } from 'react-js-cron';
import styled from 'styled-components';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { ANTD_GRAY } from '@app/entity/shared/constants';
import { DEFAULT_ASSERTION_EVALUATION_SCHEDULE } from '@app/entity/shared/tabs/Dataset/Validations/assertion/builder/constants';
import {
    getEvaluationScheduleTitle,
    getEvaluationScheduleTooltipDescription,
} from '@app/entity/shared/tabs/Dataset/Validations/assertion/builder/steps/utils';
import { adjustCronText } from '@app/entity/shared/tabs/Dataset/Validations/assertion/builder/utils';
import { getPlatformName } from '@app/entity/shared/utils';
import { TimezoneSelect } from '@app/ingest/source/builder/TimezoneSelect';
import { TruncatedTextWithTooltip } from '@app/shared/TruncatedTextWithTooltip';
import { lowerFirstLetter } from '@app/shared/textUtil';

import { AssertionType, CronSchedule } from '@types';

const TitleSection = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
`;

const Title = styled(Typography.Title)`
    && {
        margin: 0;
    }
`;

const TimezoneTitle = styled(Title)`
    text-align: right;
`;

const TooltipContainer = styled.div`
    & .ant-typography {
        color: white;
    }
`;

const Section = styled.div`
    margin-top: 12px;
    margin-bottom: 12px;
`;

const CronText = styled.div``;

const CronSuccessCheck = styled(ClockCircleOutlined)`
    color: ${ANTD_GRAY[7]};
    margin-right: 4px;
`;

const StyledTimezoneSelect = styled(TimezoneSelect)`
    width: 150px;
`;

const Row = styled.div`
    display: flex;
    justify-content: space-between;
    border-style: solid;
    border-width: 0px;
    border-top-width: 1px;
    border-bottom-width: 1px;
    border-color: #eee;
    margin-top: 24px;
    margin-bottom: 24px;
    padding-top: 16px;
    padding-bottom: 16px;
`;

const Column = styled.div`
    display: flex;
    flex-direction: column;
`;

type Props = {
    value?: CronSchedule | null;
    onChange: (newSchedule: CronSchedule) => void;
    assertionType: AssertionType;
    headerLabel?: string;
    showTimezone?: boolean;
    showAdvanced?: boolean;
    actionText?: string;
    disabled?: boolean;
};

/**
 * Builder used to construct a Cron Schedule suitable for Assertion Evaluation.
 */
export const EvaluationScheduleBuilder = ({
    value,
    onChange,
    assertionType,
    headerLabel,
    showTimezone = true,
    showAdvanced = true,
    actionText = 'Runs at',
    disabled = false,
}: Props) => {
    const { entityData } = useEntityData();
    const platformName = getPlatformName(entityData);
    const interval = value?.cron?.replaceAll(', ', '') || DEFAULT_ASSERTION_EVALUATION_SCHEDULE;
    const timezone = value?.timezone || Intl.DateTimeFormat().resolvedOptions().timeZone;
    const title = headerLabel ?? getEvaluationScheduleTitle(assertionType);
    const tooltipDescription = getEvaluationScheduleTooltipDescription(assertionType, platformName as string);

    const currentIntervalPeriodRef = useRef<PeriodType>();
    const onIntervalInitializeOrChange = (newInterval: string, extra: SetValueFunctionExtra) => {
        let cron = newInterval;

        // If initializing, do nothing
        // Else if granularity has changed, reset cron to a default for that granularity
        if (typeof currentIntervalPeriodRef.current === 'undefined') {
            currentIntervalPeriodRef.current = extra.selectedPeriod;
        } else if (currentIntervalPeriodRef.current !== extra.selectedPeriod) {
            currentIntervalPeriodRef.current = extra.selectedPeriod;
            switch (extra.selectedPeriod) {
                case 'month':
                    cron = '0 0 1 * *';
                    break;
                case 'week':
                    cron = '0 0 * * 1';
                    break;
                case 'day':
                    cron = '0 0 * * *';
                    break;
                case 'hour':
                    cron = '0 * * * *';
                    break;
                case 'minute':
                    cron = '* * * * *';
                    break;
                default:
                    break;
            }
        }
        onChange({
            cron,
            timezone,
        });
    };

    const updateTimezone = (newTimezone: string) => {
        onChange({
            cron: interval,
            timezone: newTimezone,
        });
    };

    const cronAsText = useMemo(() => {
        if (interval) {
            try {
                return {
                    text: adjustCronText(`${lowerFirstLetter(cronstrue.toString(interval, { verbose: false }))}.`),
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

    return (
        <Row style={headerLabel ? { borderTopWidth: 0 } : undefined}>
            <Column>
                <TitleSection>
                    <Title level={5}>{title}</Title>
                    <Tooltip
                        color={ANTD_GRAY[9]}
                        placement="right"
                        title={
                            <TooltipContainer>
                                <Typography.Paragraph>{tooltipDescription}</Typography.Paragraph>
                                {showAdvanced && (
                                    <Typography.Text>
                                        <b>Pro-Tip!</b> Use the <b>Advanced</b> section to configure how checks are
                                        evaluated for this dataset.
                                    </Typography.Text>
                                )}
                            </TooltipContainer>
                        }
                    >
                        <InfoCircleOutlined style={{ color: '#999' }} />
                    </Tooltip>
                </TitleSection>
                <Section>
                    <Cron
                        value={interval}
                        setValue={onIntervalInitializeOrChange}
                        clearButton={false}
                        className="cron-builder"
                        leadingZero
                        disabled={disabled}
                    />
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
                                <TruncatedTextWithTooltip
                                    text={`${actionText} ${cronAsText.text}`}
                                    style={{ color: ANTD_GRAY[7] }}
                                    maxLength={100}
                                />
                            </>
                        )}
                    </CronText>
                </Section>
            </Column>
            <Column>
                {showTimezone && (
                    <>
                        <TimezoneTitle level={5}>In Timezone</TimezoneTitle>
                        <Section>
                            <StyledTimezoneSelect value={timezone} onChange={updateTimezone} disabled={disabled} />
                        </Section>
                    </>
                )}
            </Column>
        </Row>
    );
};

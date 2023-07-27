import React, { useMemo } from 'react';
import styled from 'styled-components';
import cronstrue from 'cronstrue';
import { Typography } from 'antd';
import { CheckCircleOutlined } from '@ant-design/icons';
import { Cron } from 'react-js-cron';
import { CronSchedule } from '../../../../../../../../../../types.generated';
import { lowerFirstLetter } from '../../../../../../../../../shared/textUtil';
import { REDESIGN_COLORS } from '../../../../../../../constants';
import { TimezoneSelect } from '../../../../../../../../../ingest/source/builder/TimezoneSelect';
import { DEFAULT_ASSERTION_EVALUATION_SCHEDULE } from '../../constants';
import { adjustCronText } from '../../utils';

const Section = styled.div`
    margin-top: 12px;
    margin-bottom: 12px;
`;

const CronText = styled.div``;

const CronSuccessCheck = styled(CheckCircleOutlined)`
    color: ${REDESIGN_COLORS.BLUE};
    margin-right: 4px;
`;

const TimezoneLabel = styled.div`
    font-weight: bold;
    margin-bottom: 12px;
`;

const StyledTimezoneSelect = styled(TimezoneSelect)`
    margin-top: 12px;
`;

const Description = styled(Typography.Paragraph)`
    && {
        padding: 0px;
        margin: 0px;
    }
`;

type Props = {
    value?: CronSchedule | null;
    title?: string | null;
    onChange: (newSchedule: CronSchedule) => void;
    showTimezone?: boolean;
    actionText?: string;
    descriptionText?: string;
};

/**
 * Builder used to construct a Cron Schedule suitable for Assertion Evaluation.
 */
export const CronScheduleBuilder = ({
    value,
    title,
    onChange,
    showTimezone = true,
    actionText = 'Runs at',
    descriptionText,
}: Props) => {
    const interval = value?.cron?.replaceAll(', ', '') || DEFAULT_ASSERTION_EVALUATION_SCHEDULE;
    const timezone = value?.timezone || Intl.DateTimeFormat().resolvedOptions().timeZone;

    const updateInterval = (newInterval: string) => {
        onChange({
            cron: newInterval,
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
        <>
            <Typography.Title level={5}>{title}</Typography.Title>
            <Section>
                <Cron
                    value={interval}
                    setValue={updateInterval}
                    clearButton={false}
                    className="cron-builder"
                    leadingZero
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
                            {actionText} {cronAsText.text}
                        </>
                    )}
                </CronText>
            </Section>
            <Section>
                {showTimezone && (
                    <>
                        <TimezoneLabel>In Timezone</TimezoneLabel>
                        <StyledTimezoneSelect value={timezone} onChange={updateTimezone} />
                    </>
                )}
            </Section>
            {descriptionText && (
                <Section>
                    <Description type="secondary">{descriptionText}</Description>
                </Section>
            )}
        </>
    );
};

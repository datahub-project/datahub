import { Typography } from 'antd';
import React from 'react';
import cronstrue from 'cronstrue';
import {
    CronSchedule,
    FixedIntervalSchedule,
    FreshnessAssertionInfo,
    FreshnessAssertionScheduleType,
    FreshnessAssertionType,
} from '../../../../../../types.generated';

type Props = {
    assertionInfo: FreshnessAssertionInfo;
    monitorSchedule?: CronSchedule;
};

const createCronText = (cronSchedule: CronSchedule) => {
    const { cron, timezone } = cronSchedule;
    return `${cronstrue.toString(cron).toLocaleLowerCase()} (${timezone})`;
};

const createFixedIntervalText = (fixedIntervalSchedule: FixedIntervalSchedule, monitorSchedule?: CronSchedule) => {
    const { multiple, unit } = fixedIntervalSchedule;
    const cronText = monitorSchedule ? `, as of ${createCronText(monitorSchedule)}` : '';
    return `in the past ${multiple} ${unit.toLocaleLowerCase()}s${cronText}`;
};

/**
 * A human-readable description of an Freshness Assertion.
 */
export const FreshnessAssertionDescription = ({ assertionInfo, monitorSchedule }: Props) => {
    const scheduleType = assertionInfo.schedule?.type;
    const freshnessType = assertionInfo.type;

    return (
        <div>
            <Typography.Text>
                {freshnessType === FreshnessAssertionType.DatasetChange
                    ? 'Dataset is updated '
                    : 'Data Task is run successfully '}
                {scheduleType === FreshnessAssertionScheduleType.Cron
                    ? createCronText(assertionInfo.schedule?.cron as any)
                    : createFixedIntervalText(assertionInfo.schedule?.fixedInterval as any, monitorSchedule)}
            </Typography.Text>
        </div>
    );
};

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
};

const createCronText = (cronSchedule: CronSchedule) => {
    const { cron, timezone } = cronSchedule;
    return `${cronstrue.toString(cron).toLocaleLowerCase()} (${timezone})`;
};

const createFixedIntervalText = (fixedIntervalSchedule: FixedIntervalSchedule) => {
    const { multiple, unit } = fixedIntervalSchedule;
    return `every ${multiple} ${unit.toLocaleLowerCase()}s`;
};

/**
 * A human-readable description of an Freshness Assertion.
 */
export const FreshnessAssertionDescription = ({ assertionInfo }: Props) => {
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
                    : createFixedIntervalText(assertionInfo.schedule?.fixedInterval as any)}
            </Typography.Text>
        </div>
    );
};

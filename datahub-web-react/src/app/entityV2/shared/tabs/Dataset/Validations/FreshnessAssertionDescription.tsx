import { Typography } from 'antd';
import { Maybe } from 'graphql/jsutils/Maybe';
import React from 'react';

import { cronToString, removeTimePrefix } from '@utils/cronstrue';

import {
    CronSchedule,
    FixedIntervalSchedule,
    FreshnessAssertionInfo,
    FreshnessAssertionScheduleType,
    FreshnessAssertionType,
} from '@types';

type Props = {
    assertionInfo: FreshnessAssertionInfo;
    monitorSchedule?: Maybe<CronSchedule>;
};

const getCronAsLabel = (cronSchedule: CronSchedule) => {
    const { cron, timezone } = cronSchedule;
    if (!cron) {
        return '';
    }
    return `${removeTimePrefix(cronToString(cron).toLocaleLowerCase())} (${timezone})`;
};

/* untranslated-text -- sentence fragment, word order differs by language */
export const createCronText = (cronSchedule: CronSchedule) => {
    return `between cron windows scheduled at ${getCronAsLabel(cronSchedule)}`;
};

/* untranslated-text -- sentence fragment, word order differs by language */
export const createFixedIntervalText = (
    fixedIntervalSchedule?: FixedIntervalSchedule | null,
    monitorSchedule?: Maybe<CronSchedule>,
) => {
    if (!fixedIntervalSchedule) {
        return 'No interval found!';
    }
    const { multiple, unit } = fixedIntervalSchedule;
    const cronText = monitorSchedule ? `, as of ${getCronAsLabel(monitorSchedule)}` : '';
    return `in the past ${multiple} ${unit.toLocaleLowerCase()}s${cronText}`;
};

/* untranslated-text -- sentence fragment, word order differs by language */
export const createSinceTheLastCheckText = (monitorSchedule?: Maybe<CronSchedule>) => {
    const cronText = monitorSchedule ? `, as of ${getCronAsLabel(monitorSchedule)}` : '';
    return `since the previous check${cronText}.`;
};

/**
 * A human-readable description of an Freshness Assertion.
 */
export const FreshnessAssertionDescription = ({ assertionInfo, monitorSchedule }: Props) => {
    const scheduleType = assertionInfo.schedule?.type;
    const freshnessType = assertionInfo.type;

    /* eslint-disable i18next/no-literal-string -- (untranslated-text) All schedule texts and the sentence prefix are fragments concatenated
       to form the full description; cannot be independently translated as word order differs by language */
    let scheduleText = '';
    switch (scheduleType) {
        case FreshnessAssertionScheduleType.FixedInterval:
            scheduleText = createFixedIntervalText(assertionInfo.schedule?.fixedInterval, monitorSchedule);
            break;
        case FreshnessAssertionScheduleType.Cron:
            scheduleText = createCronText(assertionInfo.schedule?.cron as any);
            break;
        case FreshnessAssertionScheduleType.SinceTheLastCheck:
            scheduleText = createSinceTheLastCheckText(monitorSchedule);
            break;
        default:
            scheduleText = 'within an unrecognized schedule window.';
            break;
    }
    return (
        <div>
            <Typography.Text>
                {freshnessType === FreshnessAssertionType.DatasetChange
                    ? 'Table was updated '
                    : 'Data Task is run successfully '}
                {scheduleText}
            </Typography.Text>
        </div>
    );
    /* eslint-enable i18next/no-literal-string */
};

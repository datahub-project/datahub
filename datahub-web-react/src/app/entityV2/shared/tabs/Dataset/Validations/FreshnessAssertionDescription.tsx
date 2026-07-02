import { Typography } from 'antd';
import { Maybe } from 'graphql/jsutils/Maybe';
import React from 'react';
import { useTranslation } from 'react-i18next';

import { cronToString, removeTimePrefix } from '@utils/cronstrue';

import { CronSchedule, FreshnessAssertionInfo, FreshnessAssertionScheduleType, FreshnessAssertionType } from '@types';

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

/**
 * A human-readable description of an Freshness Assertion.
 *
 * Each (freshness type × schedule type) combination maps to a single full-sentence translation key so
 * translators control the word order of the whole sentence. Dynamic, non-translatable values (interval
 * size, time unit, library-generated cron labels) are interpolated as opaque slots.
 */
export const FreshnessAssertionDescription = ({ assertionInfo, monitorSchedule }: Props) => {
    const { t } = useTranslation('entity.profile.validations');
    const scheduleType = assertionInfo.schedule?.type;
    const prefix = assertionInfo.type === FreshnessAssertionType.DatasetChange ? 'datasetChange' : 'dataTask';
    const cronLabel = monitorSchedule ? getCronAsLabel(monitorSchedule) : '';

    let scheduleText: string;
    switch (scheduleType) {
        case FreshnessAssertionScheduleType.FixedInterval: {
            const fixedInterval = assertionInfo.schedule?.fixedInterval;
            if (!fixedInterval) {
                scheduleText = t(`freshnessDescription.${prefix}.noInterval`);
            } else {
                const values = {
                    multiple: fixedInterval.multiple,
                    unit: `${fixedInterval.unit.toLocaleLowerCase()}s`,
                    schedule: cronLabel,
                };
                scheduleText = monitorSchedule
                    ? t(`freshnessDescription.${prefix}.fixedIntervalWithCron`, values)
                    : t(`freshnessDescription.${prefix}.fixedInterval`, values);
            }
            break;
        }
        case FreshnessAssertionScheduleType.Cron:
            scheduleText = t(`freshnessDescription.${prefix}.cron`, {
                schedule: getCronAsLabel(assertionInfo.schedule?.cron as CronSchedule),
            });
            break;
        case FreshnessAssertionScheduleType.SinceTheLastCheck:
            scheduleText = monitorSchedule
                ? t(`freshnessDescription.${prefix}.sinceLastCheckWithCron`, { schedule: cronLabel })
                : t(`freshnessDescription.${prefix}.sinceLastCheck`);
            break;
        default:
            scheduleText = t(`freshnessDescription.${prefix}.unknown`);
            break;
    }
    return (
        <div>
            <Typography.Text>{scheduleText}</Typography.Text>
        </div>
    );
};

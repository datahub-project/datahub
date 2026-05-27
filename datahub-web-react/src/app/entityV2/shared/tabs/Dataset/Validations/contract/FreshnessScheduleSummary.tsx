import React from 'react';
import { useTranslation } from 'react-i18next';

import { capitalizeFirstLetter } from '@app/shared/textUtil';
import { cronToString } from '@utils/cronstrue';

import { CronSchedule, FreshnessAssertionSchedule, FreshnessAssertionScheduleType } from '@types';

type Props = {
    definition: FreshnessAssertionSchedule;
    evaluationSchedule?: CronSchedule; // When the assertion is run.
};

export const FreshnessScheduleSummary = ({ definition, evaluationSchedule }: Props) => {
    const { t } = useTranslation('entity.validations');
    let scheduleText = '';

    const cronStr = definition.cron?.cron ?? evaluationSchedule?.cron;
    switch (definition.type) {
        case FreshnessAssertionScheduleType.Cron:
            scheduleText = cronStr
                ? `${capitalizeFirstLetter(cronToString(cronStr))}.`
                : t('freshnessContract.scheduleUnknown');
            break;
        case FreshnessAssertionScheduleType.SinceTheLastCheck:
            scheduleText = cronStr
                ? t('freshnessContract.scheduleSinceCheckWithCron', { schedule: cronToString(cronStr).toLowerCase() })
                : t('freshnessContract.scheduleSinceCheck');
            break;
        case FreshnessAssertionScheduleType.FixedInterval:
            /* eslint-disable i18next/no-literal-string -- combines number + lowercased unit + optional schedule into a
               sentence; splitting into separate keys would break grammar in non-English languages */
            scheduleText = `In the past ${definition.fixedInterval?.multiple} ${definition.fixedInterval?.unit.toLocaleLowerCase()}s${
                cronStr ? `, as of ${cronToString(cronStr).toLowerCase()}` : ''
            }`;
            /* eslint-enable i18next/no-literal-string */
            break;
        default:
            break;
    }

    return <>{scheduleText}</>;
};

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
    const { t } = useTranslation('entity.profile.validations');
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
            /* untranslated-text -- number + unit + optional schedule fragment, word order differs by language */
            scheduleText = `In the past ${definition.fixedInterval?.multiple} ${definition.fixedInterval?.unit.toLocaleLowerCase()}s${
                cronStr ? `, as of ${cronToString(cronStr).toLowerCase()}` : ''
            }`;
            break;
        default:
            break;
    }

    return <>{scheduleText}</>;
};

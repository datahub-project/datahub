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
        case FreshnessAssertionScheduleType.FixedInterval: {
            const multiple = definition.fixedInterval?.multiple;
            const unit = `${definition.fixedInterval?.unit.toLocaleLowerCase()}s`;
            scheduleText = cronStr
                ? t('freshnessContract.scheduleFixedIntervalWithCron', {
                      multiple,
                      unit,
                      schedule: cronToString(cronStr).toLowerCase(),
                  })
                : t('freshnessContract.scheduleFixedInterval', { multiple, unit });
            break;
        }
        default:
            break;
    }

    return <>{scheduleText}</>;
};

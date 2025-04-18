import cronstrue from 'cronstrue';
import React from 'react';

import { capitalizeFirstLetter } from '@app/shared/textUtil';

import { CronSchedule, FreshnessAssertionSchedule, FreshnessAssertionScheduleType } from '@types';

type Props = {
    definition: FreshnessAssertionSchedule;
    evaluationSchedule?: CronSchedule; // When the assertion is run.
};

export const FreshnessScheduleSummary = ({ definition, evaluationSchedule }: Props) => {
    let scheduleText = '';
    const cronStr = definition.cron?.cron ?? evaluationSchedule?.cron;
    switch (definition.type) {
        case FreshnessAssertionScheduleType.Cron:
            scheduleText = cronStr
                ? `${capitalizeFirstLetter(cronstrue.toString(cronStr))}.`
                : `Unknown freshness schedule.`;
            break;
        case FreshnessAssertionScheduleType.SinceTheLastCheck:
            scheduleText = cronStr
                ? `Since the previous check, as of ${cronstrue.toString(cronStr).toLowerCase()}`
                : 'Since the previous check';
            break;
        case FreshnessAssertionScheduleType.FixedInterval:
            scheduleText = `In the past ${
                definition.fixedInterval?.multiple
            } ${definition.fixedInterval?.unit?.toLocaleLowerCase()}s${
                cronStr ? `, as of ${cronstrue.toString(cronStr).toLowerCase()}` : ''
            }`;
            break;
        default:
            break;
    }

    return <>{scheduleText}</>;
};

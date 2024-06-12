import React from 'react';
import cronstrue from 'cronstrue';
import {
    FreshnessAssertionSchedule,
    FreshnessAssertionScheduleType,
    CronSchedule,
} from '../../../../../../../types.generated';
import { capitalizeFirstLetter } from '../../../../../../shared/textUtil';

type Props = {
    definition: FreshnessAssertionSchedule;
    evaluationSchedule?: CronSchedule; // When the assertion is run.
};

export const FreshnessScheduleSummary = ({ definition, evaluationSchedule }: Props) => {
    let scheduleText = ''
    switch (definition.type) {
        case FreshnessAssertionScheduleType.Cron:
            scheduleText = `${capitalizeFirstLetter(cronstrue.toString(definition.cron?.cron as string))}.`
            break;
        case FreshnessAssertionScheduleType.SinceTheLastCheck:
            scheduleText = `Since the previous check, as of ${cronstrue.toString(definition.cron?.cron as string).toLowerCase()}`
            break;
        case FreshnessAssertionScheduleType.FixedInterval:
            scheduleText = `In the past ${definition.fixedInterval?.multiple
                } ${definition.fixedInterval?.unit.toLocaleLowerCase()}s${(evaluationSchedule &&
                    `, as of ${cronstrue.toString(evaluationSchedule.cron as string).toLowerCase()}`) ||
                ''
                }`
            break;
        default:
            break;
    }

    return <>{scheduleText}</>;
};

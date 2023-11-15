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
    const scheduleText =
        definition.type === FreshnessAssertionScheduleType.Cron
            ? `${capitalizeFirstLetter(cronstrue.toString(definition.cron?.cron as string))}.`
            : `In the past ${
                  definition.fixedInterval?.multiple
              } ${definition.fixedInterval?.unit.toLocaleLowerCase()}s${
                  (evaluationSchedule &&
                      `, as of ${cronstrue.toString(evaluationSchedule.cron as string).toLowerCase()}`) ||
                  ''
              }`;

    return <>{scheduleText}</>;
};

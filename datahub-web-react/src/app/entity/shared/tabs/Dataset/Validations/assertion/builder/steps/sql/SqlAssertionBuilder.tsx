import React from 'react';

import { EvaluationScheduleBuilder } from '@app/entity/shared/tabs/Dataset/Validations/assertion/builder/steps/common/EvaluationScheduleBuilder';
import { SqlEvaluationBuilder } from '@app/entity/shared/tabs/Dataset/Validations/assertion/builder/steps/sql/SqlEvaluationBuilder';
import { SqlQueryBuilder } from '@app/entity/shared/tabs/Dataset/Validations/assertion/builder/steps/sql/SqlQueryBuilder';
import { AssertionMonitorBuilderState } from '@app/entity/shared/tabs/Dataset/Validations/assertion/builder/types';

import { AssertionType, CronSchedule } from '@types';

type Props = {
    state: AssertionMonitorBuilderState;
    updateState: (newState: AssertionMonitorBuilderState) => void;
    disabled?: boolean;
};

export const SqlAssertionBuilder = ({ state, updateState, disabled = false }: Props) => {
    const updateAssertionSchedule = (schedule: CronSchedule) => {
        updateState({
            ...state,
            schedule,
        });
    };

    const updateAssertionStatement = (statement: string) => {
        updateState({
            ...state,
            assertion: {
                ...state.assertion,
                sqlAssertion: {
                    ...state.assertion?.sqlAssertion,
                    statement,
                },
            },
        });
    };

    return (
        <div>
            <SqlQueryBuilder
                value={state?.assertion?.sqlAssertion?.statement}
                onChange={updateAssertionStatement}
                disabled={disabled}
            />
            <SqlEvaluationBuilder value={state} onChange={updateState} disabled={disabled} />
            <EvaluationScheduleBuilder
                value={state.schedule}
                onChange={updateAssertionSchedule}
                assertionType={AssertionType.Sql}
                showAdvanced={false}
                disabled={disabled}
            />
        </div>
    );
};

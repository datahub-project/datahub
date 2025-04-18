import React from 'react';

import { EvaluationScheduleBuilder } from '../common/EvaluationScheduleBuilder';
import { SqlQueryBuilder } from './SqlQueryBuilder';
import { SqlEvaluationBuilder } from './SqlEvaluationBuilder';
import { AssertionMonitorBuilderState } from '../../types';
import { AssertionType, CronSchedule } from '../../../../../../../../../../types.generated';

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

import React from 'react';

import { EvaluationScheduleBuilder } from '../freshness/EvaluationScheduleBuilder';
import { SqlQueryBuilder } from './SqlQueryBuilder';
import { SqlEvaluationBuilder } from './SqlEvaluationBuilder';
import { AssertionActionsSection } from '../actions/AssertionActionsSection';
import { AssertionMonitorBuilderState } from '../../types';
import { AssertionType, CronSchedule } from '../../../../../../../../../../types.generated';

type Props = {
    state: AssertionMonitorBuilderState;
    updateState: (newState: AssertionMonitorBuilderState) => void;
    editing?: boolean;
};

export const SqlAssertionBuilder = ({ state, updateState, editing = true }: Props) => {
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
            <EvaluationScheduleBuilder
                value={state.schedule as CronSchedule}
                onChange={updateAssertionSchedule}
                assertionType={AssertionType.Sql}
                showAdvanced={false}
                disabled={!editing}
            />
            <SqlQueryBuilder
                value={state?.assertion?.sqlAssertion?.statement}
                onChange={updateAssertionStatement}
                disabled={!editing}
            />
            <SqlEvaluationBuilder value={state} onChange={updateState} disabled={!editing} />
            <AssertionActionsSection state={state} updateState={updateState} editing={editing} />
        </div>
    );
};

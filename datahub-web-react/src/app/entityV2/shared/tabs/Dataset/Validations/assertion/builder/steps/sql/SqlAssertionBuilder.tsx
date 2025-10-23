import React from 'react';

import { EvaluationScheduleBuilder } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/common/EvaluationScheduleBuilder';
import { SqlEvaluationBuilder } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/sql/SqlEvaluationBuilder';
import { SqlInferenceAdjuster } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/sql/SqlInferenceAdjuster';
import { SqlQueryBuilder } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/sql/SqlQueryBuilder';
import {
    SqlOperationOptionEnum,
    getOperationOption,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/sql/utils';
import { AssertionMonitorBuilderState } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/types';

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

    const sqlType = state.assertion?.sqlAssertion?.type;
    const sqlOperator = state.assertion?.sqlAssertion?.operator;
    const optionValue = sqlType && sqlOperator ? getOperationOption(sqlType, sqlOperator) : undefined;
    const isAiInferred = optionValue === SqlOperationOptionEnum.AI_INFERRED;

    return (
        <div>
            <SqlQueryBuilder
                value={state?.assertion?.sqlAssertion?.statement}
                onChange={updateAssertionStatement}
                disabled={disabled}
            />
            <SqlEvaluationBuilder value={state} onChange={updateState} disabled={disabled} />
            {isAiInferred && <SqlInferenceAdjuster state={state} updateState={updateState} disabled={disabled} />}
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

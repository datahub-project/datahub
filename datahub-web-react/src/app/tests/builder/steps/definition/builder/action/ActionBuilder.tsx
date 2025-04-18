import React from 'react';

import { Action } from '@app/tests/builder/steps/actions/types';
import { ActionTypeSelect } from '@app/tests/builder/steps/definition/builder/action/select/ActionTypeSelect';
import {
    getActionType,
    getActionTypeForName,
    getValueOptions,
} from '@app/tests/builder/steps/definition/builder/action/utils';
import { ValueSelect } from '@app/tests/builder/steps/definition/builder/property/select/ValueSelect';
import { ActionType } from '@app/tests/builder/steps/definition/builder/property/types/action';

type Props = {
    selectedAction: Action;
    onChangeAction: (newAction: Action) => void;
    actionTypes: ActionType[];
};

/**
 * This component allows you to construct an Action to use in Metadata Tests.
 */
export const ActionBuilder = ({ selectedAction, onChangeAction, actionTypes }: Props) => {
    const onChangeActionType = (newActionType: string) => {
        onChangeAction({
            type: newActionType,
            values: [],
            ...getActionTypeForName(newActionType, actionTypes)?.additionalParams,
        });
    };

    const onChangeValues = (newValues: string[]) => {
        onChangeAction({
            ...selectedAction,
            values: newValues,
        });
    };

    /**
     * Get options required for rendering the "values" input. This is a function of the selected property and
     * operator.
     */
    const valueOptions = getValueOptions(getActionType(selectedAction, actionTypes));

    return (
        <div>
            <ActionTypeSelect
                selectedActionTypeId={selectedAction.type}
                actionTypes={actionTypes}
                onChangeActionType={onChangeActionType}
            />
            {valueOptions && (
                <ValueSelect
                    selectedValues={selectedAction?.values}
                    options={valueOptions}
                    onChangeValues={onChangeValues}
                />
            )}
        </div>
    );
};

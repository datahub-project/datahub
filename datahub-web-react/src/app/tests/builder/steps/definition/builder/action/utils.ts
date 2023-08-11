import { Action } from '../../../actions/types';
import { ActionType } from '../property/types/action';
import { ValueInputType, ValueOptions, ValueTypeId } from '../property/types/values';

/**
 * Returns true if an action of the given type has a searchable set of values.
 */
const hasSearchableValueType = (actionType: ActionType): boolean => {
    return (
        (actionType.valueType === ValueTypeId.URN || actionType.valueType === ValueTypeId.URN_LIST) &&
        actionType.valueOptions?.entityTypes
    );
};

/**
 * Returns true if there is no Property
 */
const isNoInput = (actionType: ActionType): boolean => {
    return actionType.valueType === ValueTypeId.NO_VALUE;
};

export const getActionType = (action: Action, actionTypes: ActionType[]): ActionType | undefined => {
    const maybeActionTypes = actionTypes.filter((type) => action.type.toLowerCase() === type.id.toLowerCase());
    if (maybeActionTypes.length === 1) {
        return maybeActionTypes[0];
    }
    return undefined;
};

/**
 * Returns a set of ValueOptions which determines how to render
 * the value selector for a particular well-supported Action type.
 */
export const getValueOptions = (actionType: ActionType | undefined): ValueOptions | undefined => {
    if (!actionType) {
        return undefined;
    }
    if (isNoInput(actionType)) {
        return {
            inputType: ValueInputType.NONE,
            options: actionType.valueOptions,
        };
    }
    // Display an Entity Search values input.
    if (hasSearchableValueType(actionType)) {
        return {
            inputType: ValueInputType.ENTITY_SEARCH,
            options: actionType.valueOptions,
        };
    }
    // By default, just render a normal text input.
    return {
        inputType: ValueInputType.TEXT,
        options: actionType.valueOptions,
    };
};

import { AssertionMonitorBuilderState } from '../../../types';
import { getFieldAssertionTypeKey } from '../utils';

/**
 * Updates the assertion state for "value" parameter inputs
 */
export const onValueChange = (
    newValue: string,
    state: AssertionMonitorBuilderState,
    onChange: (newState: AssertionMonitorBuilderState) => void,
) => {
    const fieldAssertionType = state.assertion?.fieldAssertion?.type;
    const fieldAssertionKey = getFieldAssertionTypeKey(fieldAssertionType);
    onChange({
        ...state,
        assertion: {
            ...state.assertion,
            fieldAssertion: {
                ...state.assertion?.fieldAssertion,
                [fieldAssertionKey]: {
                    ...state.assertion?.fieldAssertion?.[fieldAssertionKey],
                    parameters: {
                        ...state.assertion?.fieldAssertion?.[fieldAssertionKey]?.parameters,
                        value: {
                            ...state.assertion?.fieldAssertion?.[fieldAssertionKey]?.parameters?.value,
                            value: newValue,
                        },
                    },
                },
            },
        },
    });
};

/**
 * Updates the assertion state for "minValue" and "maxValue" parameter inputs
 */
export const onRangeValueChange = (
    key: 'minValue' | 'maxValue',
    newValue: number | null,
    state: AssertionMonitorBuilderState,
    onChange: (newState: AssertionMonitorBuilderState) => void,
) => {
    const fieldAssertionType = state.assertion?.fieldAssertion?.type;
    const fieldAssertionKey = getFieldAssertionTypeKey(fieldAssertionType);
    onChange({
        ...state,
        assertion: {
            ...state.assertion,
            fieldAssertion: {
                ...state.assertion?.fieldAssertion,
                [fieldAssertionKey]: {
                    ...state.assertion?.fieldAssertion?.[fieldAssertionKey],
                    parameters: {
                        ...state.assertion?.fieldAssertion?.[fieldAssertionKey]?.parameters,
                        [key]: {
                            ...state.assertion?.fieldAssertion?.[fieldAssertionKey]?.parameters?.[key],
                            value: newValue?.toString() ?? '',
                        },
                    },
                },
            },
        },
    });
};

/**
 * Converts a list of values to the appropriate type
 */
export const convertValues = (values: string[], destinationType?: string) => {
    if (destinationType === 'number') {
        return values.map((value) => {
            const parsedValue = parseFloat(value);
            return Number.isNaN(parsedValue) ? value : parsedValue;
        });
    }
    return values;
};

/**
 * Updates the assertion state for parameter inputs where the value is a list of values.
 * The values are encoded as a JSON string with the appropriate type.
 */
export const onSetValueChange = (
    newValues: string[],
    state: AssertionMonitorBuilderState,
    onChange: (newState: AssertionMonitorBuilderState) => void,
    inputType?: string,
) => {
    const fieldAssertionType = state.assertion?.fieldAssertion?.type;
    const fieldAssertionKey = getFieldAssertionTypeKey(fieldAssertionType);
    const encodedValues = JSON.stringify(convertValues(newValues, inputType));

    onChange({
        ...state,
        assertion: {
            ...state.assertion,
            fieldAssertion: {
                ...state.assertion?.fieldAssertion,
                [fieldAssertionKey]: {
                    ...state.assertion?.fieldAssertion?.[fieldAssertionKey],
                    parameters: {
                        ...state.assertion?.fieldAssertion?.[fieldAssertionKey]?.parameters,
                        value: {
                            ...state.assertion?.fieldAssertion?.[fieldAssertionKey]?.parameters?.value,
                            value: encodedValues,
                        },
                    },
                },
            },
        },
    });
};

import { ActionWorkflowField, PropertyCardinality } from '@types';

export type FieldValue = string | number | boolean | Date | string[] | null;

export type FormFieldState = {
    id: string;
    values: FieldValue[];
    error?: string;
};

export type WorkflowFormState = {
    [fieldId: string]: FormFieldState;
};

/**
 * Creates initial form state based on workflow fields
 */
export function createInitialFormState(workflowFields: ActionWorkflowField[]): WorkflowFormState {
    const initialState: WorkflowFormState = {};

    workflowFields.forEach((field) => {
        initialState[field.id] = {
            id: field.id,
            values: field.cardinality === PropertyCardinality.Multiple ? [] : [null],
            error: undefined,
        };
    });

    return initialState;
}

/**
 * Resets form state to initial values
 */
export function resetFormState(workflowFields: ActionWorkflowField[]): WorkflowFormState {
    return createInitialFormState(workflowFields);
}

/**
 * Updates a field value in the form state immutably
 */
export function updateFormFieldValue(
    formState: WorkflowFormState,
    fieldId: string,
    values: FieldValue[],
): WorkflowFormState {
    return {
        ...formState,
        [fieldId]: {
            ...formState[fieldId],
            values,
            error: undefined,
        },
    };
}

/**
 * Sets an error for a specific field
 */
export function setFormFieldError(formState: WorkflowFormState, fieldId: string, error: string): WorkflowFormState {
    return {
        ...formState,
        [fieldId]: {
            ...formState[fieldId],
            error,
        },
    };
}

/**
 * Clears all errors from the form state
 */
export function clearFormErrors(formState: WorkflowFormState): WorkflowFormState {
    const clearedState: WorkflowFormState = {};

    Object.entries(formState).forEach(([fieldId, fieldState]) => {
        clearedState[fieldId] = {
            ...fieldState,
            error: undefined,
        };
    });

    return clearedState;
}

/**
 * Applies multiple field errors to the form state
 */
export function applyFormFieldErrors(
    formState: WorkflowFormState,
    fieldErrors: Record<string, string>,
): WorkflowFormState {
    let updatedState = formState;

    Object.entries(fieldErrors).forEach(([fieldId, error]) => {
        updatedState = setFormFieldError(updatedState, fieldId, error);
    });

    return updatedState;
}

/**
 * Checks if the form has any errors
 */
export function hasFormErrors(formState: WorkflowFormState): boolean {
    return Object.values(formState).some((fieldState) => fieldState.error);
}

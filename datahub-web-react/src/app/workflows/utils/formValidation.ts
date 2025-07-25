import { shouldDisplayField } from '@app/workflows/utils/fieldConditions';

import { ActionWorkflowField, ActionWorkflowFieldValueType } from '@types';

export type FieldValue = string | number | boolean | Date | string[] | null;

export type FormFieldState = {
    id: string;
    values: FieldValue[];
    error?: string;
};

export type WorkflowFormState = {
    [fieldId: string]: FormFieldState;
};

export type ValidationResult = {
    isValid: boolean;
    fieldErrors: Record<string, string>;
};

/**
 * Validates form state against workflow field requirements
 */
export function validateWorkflowForm(
    workflowFields: ActionWorkflowField[],
    formState: WorkflowFormState,
): ValidationResult {
    const fieldErrors: Record<string, string> = {};

    // Create current form values in the format expected by shouldDisplayField
    const currentFormValues: Record<string, any[]> = {};
    Object.entries(formState).forEach(([fieldId, fieldState]) => {
        currentFormValues[fieldId] = fieldState?.values || [];
    });

    // Only validate fields that are currently visible
    const visibleFields = workflowFields.filter((field) => shouldDisplayField(field, currentFormValues));

    visibleFields.forEach((field) => {
        const fieldState = formState[field.id];
        const hasValidValue =
            fieldState?.values?.some((value) => value !== null && value !== undefined && value !== '') ?? false;

        // Check required field validation
        if (field.required && !hasValidValue) {
            fieldErrors[field.id] = `${field.name} is required`;
        }

        // Additional validation based on field type
        if (hasValidValue && fieldState?.values) {
            const isValidType = validateFieldValueTypes(field.valueType, fieldState.values);
            if (!isValidType) {
                fieldErrors[field.id] = `Invalid value type for ${field.name}`;
            }
        }
    });

    return {
        isValid: Object.keys(fieldErrors).length === 0,
        fieldErrors,
    };
}

/**
 * Validates that field values match the expected type
 */
export function validateFieldValueTypes(valueType: ActionWorkflowFieldValueType, values: FieldValue[]): boolean {
    return values.every((value) => validateSingleFieldValue(valueType, value));
}

/**
 * Validates a single field value against its expected type
 */
export function validateSingleFieldValue(valueType: ActionWorkflowFieldValueType, value: FieldValue): boolean {
    if (value === null || value === undefined || value === '') return true;

    switch (valueType) {
        case ActionWorkflowFieldValueType.String:
        case ActionWorkflowFieldValueType.RichText:
        case ActionWorkflowFieldValueType.Urn:
            return typeof value === 'string';
        case ActionWorkflowFieldValueType.Number:
            return typeof value === 'number' && !Number.isNaN(value);
        case ActionWorkflowFieldValueType.Date:
            return value instanceof Date || typeof value === 'number';
        default:
            return true;
    }
}

/**
 * Gets validation errors for specific field IDs
 */
export function getFieldValidationErrors(
    fieldIds: string[],
    validationResult: ValidationResult,
): Record<string, string> {
    const errors: Record<string, string> = {};
    fieldIds.forEach((id) => {
        if (validationResult.fieldErrors[id]) {
            errors[id] = validationResult.fieldErrors[id];
        }
    });
    return errors;
}

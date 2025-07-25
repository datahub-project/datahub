import { FieldValue } from '@app/workflows/hooks/useWorkflowFormCompletion';

import { ActionWorkflowFragment } from '@graphql/actionWorkflow.generated';
import {
    ActionWorkflowField,
    ActionWorkflowFieldValueType,
    ActionWorkflowRequestFieldInput,
    PropertyValue,
    PropertyValueInput,
} from '@types';

/**
 * Convert PropertyValue objects from GraphQL to simple field values for form components
 */
export function convertPropertyValueToFieldValue(propertyValue: PropertyValue): FieldValue {
    if ('stringValue' in propertyValue && propertyValue.stringValue !== undefined) {
        return propertyValue.stringValue;
    }
    if ('numberValue' in propertyValue && propertyValue.numberValue !== undefined) {
        return propertyValue.numberValue;
    }
    return null;
}

/**
 * Convert an array of PropertyValue objects to field values
 */
export function convertPropertyValuesToFieldValues(propertyValues: PropertyValue[]): FieldValue[] {
    return propertyValues.map(convertPropertyValueToFieldValue);
}

/**
 * Convert workflow request fields to a format suitable for WorkflowRequestModal
 */
export function convertWorkflowRequestFieldsToFormData(
    fields: Array<{ id: string; values: PropertyValue[] }>,
): Record<string, FieldValue[]> {
    const result: Record<string, FieldValue[]> = {};

    fields.forEach((field) => {
        result[field.id] = convertPropertyValuesToFieldValues(field.values);
    });

    return result;
}

/**
 * Create a read-only form state for review mode
 */
export function createReadOnlyFormState(
    workflow: ActionWorkflowFragment,
    initialFormData?: {
        description?: string;
        expiresAt?: number;
        fieldValues?: Record<string, any[]>;
    },
) {
    return {
        formState: workflow.trigger?.form?.fields.reduce(
            (acc, field) => {
                acc[field.id] = {
                    values: initialFormData?.fieldValues?.[field.id] || [],
                    error: undefined,
                };
                return acc;
            },
            {} as Record<string, any>,
        ),
        description: initialFormData?.description || '',
        setDescription: () => {},
        expiresAt: initialFormData?.expiresAt,
        setExpiresAt: () => {},
        isSubmitting: false,
        updateFieldValue: () => {},
        submitWorkflow: async () => {},
        resetForm: () => {},
    };
}

/**
 * Convert form field value to PropertyValueInput for GraphQL mutations
 */
export const convertToPropertyValue = (
    valueType: ActionWorkflowFieldValueType,
    value: FieldValue,
): PropertyValueInput => {
    if (value === null || value === undefined) {
        return { stringValue: String(value) };
    }

    switch (valueType) {
        case ActionWorkflowFieldValueType.String:
        case ActionWorkflowFieldValueType.RichText:
        case ActionWorkflowFieldValueType.Urn:
            return { stringValue: String(value) };
        case ActionWorkflowFieldValueType.Number:
            return { numberValue: typeof value === 'string' ? parseFloat(value) : Number(value) };
        case ActionWorkflowFieldValueType.Date:
            return {
                numberValue: value instanceof Date ? value.getTime() : Number(value),
            };
        default:
            return { stringValue: String(value) };
    }
};

/**
 * Convert form state to ActionWorkflowRequestFieldInput array for GraphQL mutations
 */
export const convertFormStateToRequestFields = (
    workflowFields: ActionWorkflowField[],
    formState: Record<string, { values: FieldValue[] }>,
): ActionWorkflowRequestFieldInput[] => {
    return workflowFields.map((field) => {
        const fieldState = formState[field.id];
        if (!fieldState) {
            return {
                id: field.id,
                values: [],
            };
        }

        const filteredValues = fieldState.values.filter(
            (value) => value !== null && value !== undefined && value !== '',
        );

        return {
            id: field.id,
            values: filteredValues.map((value) => convertToPropertyValue(field.valueType, value)),
        };
    });
};

import { ActionWorkflowField, ActionWorkflowFieldConditionType, FilterOperator } from '@types';

// Use the same FieldValue type as the existing hook
export type FieldValue = string | number | boolean | Date | string[] | null;

/**
 * Evaluates whether a field should be displayed based on its condition and current form values
 */
export function shouldDisplayField(field: ActionWorkflowField, formValues: Record<string, FieldValue[]>): boolean {
    // If no condition, always display the field
    if (!field.condition) {
        return true;
    }

    // Currently only SINGLE_FIELD_VALUE is supported
    if (field.condition.type === ActionWorkflowFieldConditionType.SingleFieldValue) {
        const singleCondition = field.condition.singleFieldValueCondition;
        if (!singleCondition) {
            return true;
        }

        // Get the current values for the referenced field
        const referencedFieldValues = formValues[singleCondition.field] || [];

        // Convert all values to strings for comparison (as conditions use string values)
        const currentStringValues = referencedFieldValues
            .filter((val) => val !== null && val !== undefined)
            .map((val) => convertValueToString(val));

        // Check if any current value matches the condition
        const hasMatch = evaluateCondition(currentStringValues, singleCondition.values, singleCondition.condition);

        // Apply negation if specified
        return singleCondition.negated ? !hasMatch : hasMatch;
    }

    // Unknown condition type - default to showing the field
    return true;
}

/**
 * Converts a FieldValue to string for comparison
 */
function convertValueToString(value: FieldValue): string {
    if (value === null || value === undefined) {
        return '';
    }

    if (Array.isArray(value)) {
        // For string arrays, join with comma or check each element
        return value.join(',');
    }

    if (value instanceof Date) {
        return value.toISOString();
    }

    if (typeof value === 'boolean') {
        return value.toString();
    }

    return String(value);
}

/**
 * Evaluates a condition based on the operator
 */
function evaluateCondition(currentValues: string[], conditionValues: string[], operator: FilterOperator): boolean {
    if (currentValues.length === 0 || conditionValues.length === 0) {
        return false;
    }

    // Check if any current value matches any condition value based on the operator
    return currentValues.some((currentValue) =>
        conditionValues.some((conditionValue) => evaluateSingleCondition(currentValue, conditionValue, operator)),
    );
}

/**
 * Evaluates a single value against a condition value using the specified operator
 */
function evaluateSingleCondition(currentValue: string, conditionValue: string, operator: FilterOperator): boolean {
    switch (operator) {
        case FilterOperator.Equal:
            return currentValue === conditionValue;

        case FilterOperator.Contain:
            return currentValue.includes(conditionValue);

        case FilterOperator.StartWith:
            return currentValue.startsWith(conditionValue);

        case FilterOperator.EndWith:
            return currentValue.endsWith(conditionValue);

        case FilterOperator.In:
            // For IN operator, check if current value is in the condition values array
            return conditionValue
                .split(',')
                .map((v) => v.trim())
                .includes(currentValue);

        case FilterOperator.GreaterThan:
        case FilterOperator.GreaterThanOrEqualTo:
        case FilterOperator.LessThan:
        case FilterOperator.LessThanOrEqualTo: {
            // Try numeric comparison for number operators
            const currentNum = parseFloat(currentValue);
            const conditionNum = parseFloat(conditionValue);
            if (!Number.isNaN(currentNum) && !Number.isNaN(conditionNum)) {
                switch (operator) {
                    case FilterOperator.GreaterThan:
                        return currentNum > conditionNum;
                    case FilterOperator.GreaterThanOrEqualTo:
                        return currentNum >= conditionNum;
                    case FilterOperator.LessThan:
                        return currentNum < conditionNum;
                    case FilterOperator.LessThanOrEqualTo:
                        return currentNum <= conditionNum;
                    default:
                        return false;
                }
            }
            return false;
        }

        case FilterOperator.Exists:
            // For EXISTS, just check if current value is not empty
            return currentValue.trim() !== '';

        default:
            // Default to EQUAL for unknown operators
            return currentValue === conditionValue;
    }
}

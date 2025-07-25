import { FieldValue } from '@app/workflows/hooks/useWorkflowFormCompletion';

import { ActionWorkflowField, ActionWorkflowFieldValueType, PropertyCardinality } from '@types';

/**
 * Processes field allowed values into select options format
 */
export function processFieldAllowedValues(field: ActionWorkflowField) {
    return (
        field.allowedValues?.map((val) => {
            // Use inline fragments to access PropertyValue union type
            if ('stringValue' in val) {
                return {
                    label: val.stringValue,
                    value: val.stringValue,
                };
            }
            if ('numberValue' in val) {
                return {
                    label: val.numberValue.toString(),
                    value: val.numberValue.toString(),
                };
            }
            return {
                label: 'Unknown',
                value: '',
            };
        }) || []
    );
}

/**
 * Generates placeholder text for form fields
 */
export function getFieldPlaceholder(fieldName: string, isReviewMode: boolean): string {
    if (isReviewMode) {
        return 'No value provided';
    }
    return `Provide ${fieldName.toLowerCase()}`;
}

/**
 * Determines if field has predefined allowed values
 */
export function hasAllowedValues(field: ActionWorkflowField): boolean {
    const allowedValues = processFieldAllowedValues(field);
    return allowedValues.length > 0;
}

/**
 * Determines if field supports multiple values
 */
export function isMultipleField(field: ActionWorkflowField): boolean {
    return field.cardinality === PropertyCardinality.Multiple;
}

/**
 * Determines the appropriate field type for rendering
 */
export function getFieldInputType(field: ActionWorkflowField): 'urn' | 'select' | 'input' {
    if (field.valueType === ActionWorkflowFieldValueType.Urn) {
        return 'urn';
    }
    if (hasAllowedValues(field)) {
        return 'select';
    }
    return 'input';
}

/**
 * Creates change handlers for field values
 *
 * TODO: Remove _isMultiple parameter
 */
export function createFieldChangeHandlers(
    values: FieldValue[],
    onChange: (values: FieldValue[]) => void,
    _isMultiple: boolean,
) {
    const handleSingleChange = (value: FieldValue) => {
        onChange([value]);
    };

    const handleMultipleChange = (index: number, value: FieldValue) => {
        const newValues = [...values];
        newValues[index] = value;
        onChange(newValues);
    };

    const addValue = () => {
        onChange([...values, null]);
    };

    const removeValue = (index: number) => {
        const newValues = values.filter((_, i) => i !== index);
        onChange(newValues);
    };

    return {
        handleSingleChange,
        handleMultipleChange,
        addValue,
        removeValue,
    };
}

/**
 * Filters and prepares values for select components
 */
export function prepareSelectValues(values: FieldValue[]): string[] {
    return values.filter((v) => v !== null && v !== undefined && v !== '').map((v) => String(v));
}

/**
 * Filters and prepares URN values for EntitySearchSelect
 */
export function prepareUrnValues(values: FieldValue[]): string[] {
    return values.filter((v) => v && typeof v === 'string').map((v) => String(v));
}

/**
 * Generates test ID for field components to match original format exactly
 */
export function getFieldTestId(fieldId: string, suffix?: string, index?: number): string {
    let testId = `workflow-field-${fieldId}`;

    // Index comes directly after field ID (original behavior)
    if (index !== undefined) {
        testId += `-${index}`;
    }

    // Suffix comes last
    if (suffix) {
        testId += `-${suffix}`;
    }

    return testId;
}

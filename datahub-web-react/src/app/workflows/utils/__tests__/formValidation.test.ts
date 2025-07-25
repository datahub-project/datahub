import {
    type WorkflowFormState,
    getFieldValidationErrors,
    validateFieldValueTypes,
    validateSingleFieldValue,
    validateWorkflowForm,
} from '@app/workflows/utils/formValidation';

import {
    ActionWorkflowField,
    ActionWorkflowFieldConditionType,
    ActionWorkflowFieldValueType,
    FilterOperator,
    PropertyCardinality,
} from '@types';

describe('formValidation', () => {
    const createMockField = (overrides: Partial<ActionWorkflowField> = {}): ActionWorkflowField => ({
        id: 'test-field',
        name: 'Test Field',
        valueType: ActionWorkflowFieldValueType.String,
        cardinality: PropertyCardinality.Single,
        required: true,
        ...overrides,
    });

    const createMockFormState = (fieldValues: Record<string, any[]>): WorkflowFormState => {
        const formState: WorkflowFormState = {};
        Object.entries(fieldValues).forEach(([fieldId, values]) => {
            formState[fieldId] = {
                id: fieldId,
                values,
                error: undefined,
            };
        });
        return formState;
    };

    describe('validateSingleFieldValue', () => {
        it('should validate string values correctly', () => {
            expect(validateSingleFieldValue(ActionWorkflowFieldValueType.String, 'test')).toBe(true);
            expect(validateSingleFieldValue(ActionWorkflowFieldValueType.String, 123)).toBe(false);
            expect(validateSingleFieldValue(ActionWorkflowFieldValueType.String, null)).toBe(true);
            expect(validateSingleFieldValue(ActionWorkflowFieldValueType.String, '')).toBe(true);
        });

        it('should validate number values correctly', () => {
            expect(validateSingleFieldValue(ActionWorkflowFieldValueType.Number, 123)).toBe(true);
            expect(validateSingleFieldValue(ActionWorkflowFieldValueType.Number, 123.45)).toBe(true);
            expect(validateSingleFieldValue(ActionWorkflowFieldValueType.Number, 'not-a-number')).toBe(false);
            expect(validateSingleFieldValue(ActionWorkflowFieldValueType.Number, NaN)).toBe(false);
        });

        it('should validate date values correctly', () => {
            const date = new Date();
            const timestamp = Date.now();

            expect(validateSingleFieldValue(ActionWorkflowFieldValueType.Date, date)).toBe(true);
            expect(validateSingleFieldValue(ActionWorkflowFieldValueType.Date, timestamp)).toBe(true);
            expect(validateSingleFieldValue(ActionWorkflowFieldValueType.Date, 'not-a-date')).toBe(false);
        });

        it('should validate URN values correctly', () => {
            expect(validateSingleFieldValue(ActionWorkflowFieldValueType.Urn, 'urn:li:dataset:123')).toBe(true);
            expect(validateSingleFieldValue(ActionWorkflowFieldValueType.Urn, 123)).toBe(false);
        });
    });

    describe('validateFieldValueTypes', () => {
        it('should validate array of string values', () => {
            expect(validateFieldValueTypes(ActionWorkflowFieldValueType.String, ['test1', 'test2'])).toBe(true);
            expect(validateFieldValueTypes(ActionWorkflowFieldValueType.String, ['test1', 123])).toBe(false);
            expect(validateFieldValueTypes(ActionWorkflowFieldValueType.String, [])).toBe(true);
        });

        it('should validate array of number values', () => {
            expect(validateFieldValueTypes(ActionWorkflowFieldValueType.Number, [123, 456.78])).toBe(true);
            expect(validateFieldValueTypes(ActionWorkflowFieldValueType.Number, [123, 'not-number'])).toBe(false);
        });
    });

    describe('validateWorkflowForm', () => {
        it('should validate required fields correctly', () => {
            const fields = [
                createMockField({ id: 'required-field', required: true }),
                createMockField({ id: 'optional-field', required: false }),
            ];

            const formState = createMockFormState({
                'required-field': ['value'],
                'optional-field': [],
            });

            const result = validateWorkflowForm(fields, formState);
            expect(result.isValid).toBe(true);
            expect(Object.keys(result.fieldErrors)).toHaveLength(0);
        });

        it('should return errors for missing required fields', () => {
            const fields = [createMockField({ id: 'required-field', name: 'Required Field', required: true })];

            const formState = createMockFormState({
                'required-field': [null],
            });

            const result = validateWorkflowForm(fields, formState);
            expect(result.isValid).toBe(false);
            expect(result.fieldErrors['required-field']).toBe('Required Field is required');
        });

        it('should validate field types', () => {
            const fields = [
                createMockField({
                    id: 'number-field',
                    name: 'Number Field',
                    valueType: ActionWorkflowFieldValueType.Number,
                    required: false,
                }),
            ];

            const formState = createMockFormState({
                'number-field': ['not-a-number'],
            });

            const result = validateWorkflowForm(fields, formState);
            expect(result.isValid).toBe(false);
            expect(result.fieldErrors['number-field']).toBe('Invalid value type for Number Field');
        });

        it('should skip validation for hidden conditional fields', () => {
            const fields = [
                createMockField({ id: 'trigger-field', required: false }),
                createMockField({
                    id: 'conditional-field',
                    name: 'Conditional Field',
                    required: true,
                    condition: {
                        type: ActionWorkflowFieldConditionType.SingleFieldValue,
                        singleFieldValueCondition: {
                            field: 'trigger-field',
                            values: ['SHOW'],
                            condition: FilterOperator.Equal,
                            negated: false,
                        },
                    },
                }),
            ];

            const formState = createMockFormState({
                'trigger-field': ['HIDE'], // This should hide the conditional field
                'conditional-field': [null], // Empty but required
            });

            const result = validateWorkflowForm(fields, formState);
            expect(result.isValid).toBe(true); // Should be valid because conditional field is hidden
        });

        it('should validate visible conditional fields', () => {
            const fields = [
                createMockField({ id: 'trigger-field', required: false }),
                createMockField({
                    id: 'conditional-field',
                    name: 'Conditional Field',
                    required: true,
                    condition: {
                        type: ActionWorkflowFieldConditionType.SingleFieldValue,
                        singleFieldValueCondition: {
                            field: 'trigger-field',
                            values: ['SHOW'],
                            condition: FilterOperator.Equal,
                            negated: false,
                        },
                    },
                }),
            ];

            const formState = createMockFormState({
                'trigger-field': ['SHOW'], // This should show the conditional field
                'conditional-field': [null], // Empty but required
            });

            const result = validateWorkflowForm(fields, formState);
            expect(result.isValid).toBe(false);
            expect(result.fieldErrors['conditional-field']).toBe('Conditional Field is required');
        });
    });

    describe('getFieldValidationErrors', () => {
        it('should extract errors for specific field IDs', () => {
            const validationResult = {
                isValid: false,
                fieldErrors: {
                    field1: 'Error 1',
                    field2: 'Error 2',
                    field3: 'Error 3',
                },
            };

            const errors = getFieldValidationErrors(['field1', 'field3'], validationResult);
            expect(errors).toEqual({
                field1: 'Error 1',
                field3: 'Error 3',
            });
        });

        it('should return empty object for non-existent field IDs', () => {
            const validationResult = {
                isValid: false,
                fieldErrors: {
                    field1: 'Error 1',
                },
            };

            const errors = getFieldValidationErrors(['field2', 'field3'], validationResult);
            expect(errors).toEqual({});
        });
    });
});

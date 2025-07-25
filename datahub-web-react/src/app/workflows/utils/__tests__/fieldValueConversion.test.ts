import {
    convertFormStateToRequestFields,
    convertPropertyValueToFieldValue,
    convertPropertyValuesToFieldValues,
    convertToPropertyValue,
    convertWorkflowRequestFieldsToFormData,
    createReadOnlyFormState,
} from '@app/workflows/utils/fieldValueConversion';

import { ActionWorkflowFragment } from '@graphql/actionWorkflow.generated';
import {
    ActionWorkflowCategory,
    ActionWorkflowFieldValueType,
    ActionWorkflowTriggerType,
    PropertyCardinality,
    PropertyValue,
} from '@types';

describe('fieldValueConversion', () => {
    describe('convertPropertyValueToFieldValue', () => {
        it('should convert string property values', () => {
            const propertyValue: PropertyValue = { stringValue: 'test-string' };
            expect(convertPropertyValueToFieldValue(propertyValue)).toBe('test-string');
        });

        it('should convert number property values', () => {
            const propertyValue: PropertyValue = { numberValue: 123.45 };
            expect(convertPropertyValueToFieldValue(propertyValue)).toBe(123.45);
        });

        it('should return null for empty property values', () => {
            const propertyValue = {} as PropertyValue;
            expect(convertPropertyValueToFieldValue(propertyValue)).toBeNull();
        });

        it('should return null for property values with undefined values', () => {
            const propertyValue = { stringValue: undefined, numberValue: undefined } as any;
            expect(convertPropertyValueToFieldValue(propertyValue)).toBeNull();
        });
    });

    describe('convertPropertyValuesToFieldValues', () => {
        it('should convert array of property values', () => {
            const propertyValues: PropertyValue[] = [
                { stringValue: 'value1' },
                { numberValue: 42 },
                { stringValue: 'value2' },
                {} as PropertyValue,
            ];

            const result = convertPropertyValuesToFieldValues(propertyValues);
            expect(result).toEqual(['value1', 42, 'value2', null]);
        });

        it('should handle empty array', () => {
            expect(convertPropertyValuesToFieldValues([])).toEqual([]);
        });
    });

    describe('convertWorkflowRequestFieldsToFormData', () => {
        it('should convert request fields to form data format', () => {
            const fields = [
                {
                    id: 'field1',
                    values: [{ stringValue: 'value1' }, { stringValue: 'value2' }],
                },
                {
                    id: 'field2',
                    values: [{ numberValue: 123 }],
                },
            ];

            const result = convertWorkflowRequestFieldsToFormData(fields);
            expect(result).toEqual({
                field1: ['value1', 'value2'],
                field2: [123],
            });
        });
    });

    describe('convertToPropertyValue', () => {
        it('should convert string values', () => {
            const result = convertToPropertyValue(ActionWorkflowFieldValueType.String, 'test-string');
            expect(result).toEqual({ stringValue: 'test-string' });
        });

        it('should convert rich text values', () => {
            const result = convertToPropertyValue(ActionWorkflowFieldValueType.RichText, 'rich-text');
            expect(result).toEqual({ stringValue: 'rich-text' });
        });

        it('should convert URN values', () => {
            const result = convertToPropertyValue(ActionWorkflowFieldValueType.Urn, 'urn:li:dataset:123');
            expect(result).toEqual({ stringValue: 'urn:li:dataset:123' });
        });

        it('should convert number values', () => {
            const result = convertToPropertyValue(ActionWorkflowFieldValueType.Number, 123.45);
            expect(result).toEqual({ numberValue: 123.45 });
        });

        it('should convert string numbers to number values', () => {
            const result = convertToPropertyValue(ActionWorkflowFieldValueType.Number, '123');
            expect(result).toEqual({ numberValue: 123 });
        });

        it('should convert Date objects to timestamps', () => {
            const date = new Date('2023-01-01T00:00:00.000Z');
            const result = convertToPropertyValue(ActionWorkflowFieldValueType.Date, date);
            expect(result).toEqual({ numberValue: date.getTime() });
        });

        it('should convert number timestamps as-is', () => {
            const timestamp = Date.now();
            const result = convertToPropertyValue(ActionWorkflowFieldValueType.Date, timestamp);
            expect(result).toEqual({ numberValue: timestamp });
        });

        it('should convert unknown types to string', () => {
            const result = convertToPropertyValue('UNKNOWN_TYPE' as any, 'some-value');
            expect(result).toEqual({ stringValue: 'some-value' });
        });

        it('should handle null and undefined values', () => {
            const result = convertToPropertyValue(ActionWorkflowFieldValueType.String, null);
            expect(result).toEqual({ stringValue: 'null' });
        });
    });

    describe('convertFormStateToRequestFields', () => {
        const mockWorkflowFields = [
            {
                id: 'string-field',
                name: 'String Field',
                valueType: ActionWorkflowFieldValueType.String,
                cardinality: PropertyCardinality.Single,
                required: true,
            },
            {
                id: 'number-field',
                name: 'Number Field',
                valueType: ActionWorkflowFieldValueType.Number,
                cardinality: PropertyCardinality.Single,
                required: false,
            },
            {
                id: 'multiple-field',
                name: 'Multiple Field',
                valueType: ActionWorkflowFieldValueType.String,
                cardinality: PropertyCardinality.Multiple,
                required: false,
            },
        ];

        it('should convert form state to request fields', () => {
            const formState = {
                'string-field': { values: ['test-value'] },
                'number-field': { values: [123] },
                'multiple-field': { values: ['value1', 'value2'] },
            };

            const result = convertFormStateToRequestFields(mockWorkflowFields, formState);

            expect(result).toEqual([
                {
                    id: 'string-field',
                    values: [{ stringValue: 'test-value' }],
                },
                {
                    id: 'number-field',
                    values: [{ numberValue: 123 }],
                },
                {
                    id: 'multiple-field',
                    values: [{ stringValue: 'value1' }, { stringValue: 'value2' }],
                },
            ]);
        });

        it('should filter out null, undefined, and empty string values', () => {
            const formState = {
                'string-field': {
                    values: ['valid-value', null, 'another-valid'].filter((v) => v !== null) as string[],
                },
            };

            const result = convertFormStateToRequestFields([mockWorkflowFields[0]], formState);

            expect(result).toEqual([
                {
                    id: 'string-field',
                    values: [{ stringValue: 'valid-value' }, { stringValue: 'another-valid' }],
                },
            ]);
        });

        it('should handle empty field values', () => {
            const formState = {
                'string-field': { values: [] },
                'number-field': { values: [] },
            };

            const result = convertFormStateToRequestFields([mockWorkflowFields[0], mockWorkflowFields[1]], formState);

            expect(result).toEqual([
                {
                    id: 'string-field',
                    values: [],
                },
                {
                    id: 'number-field',
                    values: [],
                },
            ]);
        });
    });

    describe('createReadOnlyFormState', () => {
        const mockWorkflow = {
            urn: 'urn:li:workflow:123',
            name: 'Test Workflow',
            category: ActionWorkflowCategory.Access,
            trigger: {
                type: ActionWorkflowTriggerType.FormSubmitted,
                form: {
                    fields: [
                        {
                            id: 'field1',
                            name: 'Field 1',
                            valueType: ActionWorkflowFieldValueType.String,
                            cardinality: PropertyCardinality.Single,
                            required: true,
                        },
                        {
                            id: 'field2',
                            name: 'Field 2',
                            valueType: ActionWorkflowFieldValueType.Number,
                            cardinality: PropertyCardinality.Multiple,
                            required: false,
                        },
                    ],
                },
            },
        } as ActionWorkflowFragment;

        it('should create read-only form state with initial data', () => {
            const initialData = {
                description: 'Test description',
                expiresAt: 1234567890,
                fieldValues: {
                    field1: ['value1'],
                    field2: [123, 456],
                },
            };

            const result = createReadOnlyFormState(mockWorkflow, initialData);

            expect(result.formState).toEqual({
                field1: {
                    values: ['value1'],
                    error: undefined,
                },
                field2: {
                    values: [123, 456],
                    error: undefined,
                },
            });
            expect(result.description).toBe('Test description');
            expect(result.expiresAt).toBe(1234567890);
            expect(result.isSubmitting).toBe(false);
        });

        it('should create read-only form state without initial data', () => {
            const result = createReadOnlyFormState(mockWorkflow);

            expect(result.formState).toEqual({
                field1: {
                    values: [],
                    error: undefined,
                },
                field2: {
                    values: [],
                    error: undefined,
                },
            });
            expect(result.description).toBe('');
            expect(result.expiresAt).toBeUndefined();
        });

        it('should have no-op functions for read-only mode', () => {
            const result = createReadOnlyFormState(mockWorkflow);

            // These should not throw errors and should do nothing
            expect(() => result.setDescription()).not.toThrow();
            expect(() => result.setExpiresAt()).not.toThrow();
            expect(() => result.updateFieldValue()).not.toThrow();
            expect(() => result.resetForm()).not.toThrow();
            expect(() => result.submitWorkflow()).not.toThrow();
        });
    });
});

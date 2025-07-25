import { vi } from 'vitest';

import { FieldValue } from '@app/workflows/hooks/useWorkflowFormCompletion';
import {
    createFieldChangeHandlers,
    getFieldInputType,
    getFieldPlaceholder,
    getFieldTestId,
    hasAllowedValues,
    isMultipleField,
    prepareSelectValues,
    prepareUrnValues,
    processFieldAllowedValues,
} from '@app/workflows/utils/fieldInputHelpers';

import { ActionWorkflowField, ActionWorkflowFieldValueType, PropertyCardinality, PropertyValue } from '@types';

describe('fieldInputHelpers', () => {
    describe('processFieldAllowedValues', () => {
        it('should process string allowed values correctly', () => {
            const field: Partial<ActionWorkflowField> = {
                allowedValues: [
                    { stringValue: 'option1' } as PropertyValue,
                    { stringValue: 'option2' } as PropertyValue,
                ],
            };

            const result = processFieldAllowedValues(field as ActionWorkflowField);
            expect(result).toEqual([
                { label: 'option1', value: 'option1' },
                { label: 'option2', value: 'option2' },
            ]);
        });

        it('should process number allowed values correctly', () => {
            const field: Partial<ActionWorkflowField> = {
                allowedValues: [{ numberValue: 123 } as PropertyValue, { numberValue: 456.78 } as PropertyValue],
            };

            const result = processFieldAllowedValues(field as ActionWorkflowField);
            expect(result).toEqual([
                { label: '123', value: '123' },
                { label: '456.78', value: '456.78' },
            ]);
        });

        it('should handle unknown value types', () => {
            const field: Partial<ActionWorkflowField> = {
                allowedValues: [
                    {} as PropertyValue, // Unknown type
                ],
            };

            const result = processFieldAllowedValues(field as ActionWorkflowField);
            expect(result).toEqual([{ label: 'Unknown', value: '' }]);
        });

        it('should return empty array when no allowed values', () => {
            const field: Partial<ActionWorkflowField> = {
                allowedValues: undefined,
            };

            const result = processFieldAllowedValues(field as ActionWorkflowField);
            expect(result).toEqual([]);
        });

        it('should return empty array when allowed values is empty array', () => {
            const field: Partial<ActionWorkflowField> = {
                allowedValues: [],
            };

            const result = processFieldAllowedValues(field as ActionWorkflowField);
            expect(result).toEqual([]);
        });
    });

    describe('getFieldPlaceholder', () => {
        it('should return "No value provided" in review mode', () => {
            const result = getFieldPlaceholder('TestField', true);
            expect(result).toBe('No value provided');
        });

        it('should return "Provide fieldname" in non-review mode', () => {
            const result = getFieldPlaceholder('TestField', false);
            expect(result).toBe('Provide testfield');
        });

        it('should handle field names with spaces', () => {
            const result = getFieldPlaceholder('Test Field Name', false);
            expect(result).toBe('Provide test field name');
        });

        it('should handle uppercase field names', () => {
            const result = getFieldPlaceholder('UPPERCASE', false);
            expect(result).toBe('Provide uppercase');
        });
    });

    describe('hasAllowedValues', () => {
        it('should return true when field has allowed values', () => {
            const field: Partial<ActionWorkflowField> = {
                allowedValues: [{ stringValue: 'option1' } as PropertyValue],
            };

            expect(hasAllowedValues(field as ActionWorkflowField)).toBe(true);
        });

        it('should return false when field has no allowed values', () => {
            const field: Partial<ActionWorkflowField> = {
                allowedValues: undefined,
            };

            expect(hasAllowedValues(field as ActionWorkflowField)).toBe(false);
        });

        it('should return false when field has empty allowed values array', () => {
            const field: Partial<ActionWorkflowField> = {
                allowedValues: [],
            };

            expect(hasAllowedValues(field as ActionWorkflowField)).toBe(false);
        });
    });

    describe('isMultipleField', () => {
        it('should return true for multiple cardinality', () => {
            const field: Partial<ActionWorkflowField> = {
                cardinality: PropertyCardinality.Multiple,
            };

            expect(isMultipleField(field as ActionWorkflowField)).toBe(true);
        });

        it('should return false for single cardinality', () => {
            const field: Partial<ActionWorkflowField> = {
                cardinality: PropertyCardinality.Single,
            };

            expect(isMultipleField(field as ActionWorkflowField)).toBe(false);
        });
    });

    describe('getFieldInputType', () => {
        it('should return "urn" for URN value type', () => {
            const field: Partial<ActionWorkflowField> = {
                valueType: ActionWorkflowFieldValueType.Urn,
                allowedValues: undefined,
            };

            expect(getFieldInputType(field as ActionWorkflowField)).toBe('urn');
        });

        it('should return "select" for fields with allowed values', () => {
            const field: Partial<ActionWorkflowField> = {
                valueType: ActionWorkflowFieldValueType.String,
                allowedValues: [{ stringValue: 'option1' } as PropertyValue],
            };

            expect(getFieldInputType(field as ActionWorkflowField)).toBe('select');
        });

        it('should return "input" for regular fields without allowed values', () => {
            const field: Partial<ActionWorkflowField> = {
                valueType: ActionWorkflowFieldValueType.String,
                allowedValues: undefined,
            };

            expect(getFieldInputType(field as ActionWorkflowField)).toBe('input');
        });

        it('should prioritize URN over allowed values', () => {
            const field: Partial<ActionWorkflowField> = {
                valueType: ActionWorkflowFieldValueType.Urn,
                allowedValues: [{ stringValue: 'option1' } as PropertyValue],
            };

            expect(getFieldInputType(field as ActionWorkflowField)).toBe('urn');
        });
    });

    describe('createFieldChangeHandlers', () => {
        let mockOnChange: ReturnType<typeof vi.fn>;

        beforeEach(() => {
            mockOnChange = vi.fn();
        });

        describe('handleSingleChange', () => {
            it('should wrap single value in array', () => {
                const { handleSingleChange } = createFieldChangeHandlers([], mockOnChange, false);

                handleSingleChange('test-value');

                expect(mockOnChange).toHaveBeenCalledWith(['test-value']);
            });
        });

        describe('handleMultipleChange', () => {
            it('should update value at specific index', () => {
                const values = ['value1', 'value2', 'value3'];
                const { handleMultipleChange } = createFieldChangeHandlers(values, mockOnChange, true);

                handleMultipleChange(1, 'new-value');

                expect(mockOnChange).toHaveBeenCalledWith(['value1', 'new-value', 'value3']);
            });

            it('should not mutate original array', () => {
                const values = ['value1', 'value2'];
                const { handleMultipleChange } = createFieldChangeHandlers(values, mockOnChange, true);

                handleMultipleChange(0, 'new-value');

                expect(values).toEqual(['value1', 'value2']); // Original unchanged
                expect(mockOnChange).toHaveBeenCalledWith(['new-value', 'value2']);
            });
        });

        describe('addValue', () => {
            it('should add null value to array', () => {
                const values = ['value1', 'value2'];
                const { addValue } = createFieldChangeHandlers(values, mockOnChange, true);

                addValue();

                expect(mockOnChange).toHaveBeenCalledWith(['value1', 'value2', null]);
            });

            it('should handle empty array', () => {
                const values: any[] = [];
                const { addValue } = createFieldChangeHandlers(values, mockOnChange, true);

                addValue();

                expect(mockOnChange).toHaveBeenCalledWith([null]);
            });
        });

        describe('removeValue', () => {
            it('should remove value at specific index', () => {
                const values = ['value1', 'value2', 'value3'];
                const { removeValue } = createFieldChangeHandlers(values, mockOnChange, true);

                removeValue(1);

                expect(mockOnChange).toHaveBeenCalledWith(['value1', 'value3']);
            });

            it('should handle removing first element', () => {
                const values = ['value1', 'value2'];
                const { removeValue } = createFieldChangeHandlers(values, mockOnChange, true);

                removeValue(0);

                expect(mockOnChange).toHaveBeenCalledWith(['value2']);
            });

            it('should handle removing last element', () => {
                const values = ['value1', 'value2'];
                const { removeValue } = createFieldChangeHandlers(values, mockOnChange, true);

                removeValue(1);

                expect(mockOnChange).toHaveBeenCalledWith(['value1']);
            });

            it('should return empty array when removing single element', () => {
                const values = ['value1'];
                const { removeValue } = createFieldChangeHandlers(values, mockOnChange, true);

                removeValue(0);

                expect(mockOnChange).toHaveBeenCalledWith([]);
            });
        });
    });

    describe('prepareSelectValues', () => {
        it('should filter out null, undefined, and empty string values', () => {
            const values = ['value1', null, 'value2', undefined, '', 'value3'] as FieldValue[];

            const result = prepareSelectValues(values);

            expect(result).toEqual(['value1', 'value2', 'value3']);
        });

        it('should convert all values to strings', () => {
            const values = ['string', 123, true, { toString: () => 'object' }] as FieldValue[];

            const result = prepareSelectValues(values);

            expect(result).toEqual(['string', '123', 'true', 'object']);
        });

        it('should handle empty array', () => {
            expect(prepareSelectValues([])).toEqual([]);
        });

        it('should handle array with only invalid values', () => {
            const values = [null, undefined, ''] as FieldValue[];

            const result = prepareSelectValues(values);

            expect(result).toEqual([]);
        });
    });

    describe('prepareUrnValues', () => {
        it('should filter and convert URN values', () => {
            const values = [
                'urn:li:dataset:1',
                null,
                'urn:li:dataset:2',
                undefined,
                123,
                'urn:li:dataset:3',
            ] as FieldValue[];

            const result = prepareUrnValues(values);

            expect(result).toEqual(['urn:li:dataset:1', 'urn:li:dataset:2', 'urn:li:dataset:3']);
        });

        it('should filter out non-string values', () => {
            const values = ['valid-urn', 123, true, null, undefined] as FieldValue[];

            const result = prepareUrnValues(values);

            expect(result).toEqual(['valid-urn']);
        });

        it('should handle empty array', () => {
            expect(prepareUrnValues([])).toEqual([]);
        });

        it('should handle array with no valid URNs', () => {
            const values = [null, undefined, 123, true] as FieldValue[];

            const result = prepareUrnValues(values);

            expect(result).toEqual([]);
        });
    });

    describe('getFieldTestId', () => {
        it('should generate basic test ID', () => {
            const result = getFieldTestId('field-123');
            expect(result).toBe('workflow-field-field-123');
        });

        it('should include index when provided', () => {
            const result = getFieldTestId('field-123', undefined, 2);
            expect(result).toBe('workflow-field-field-123-2');
        });

        it('should include suffix when provided', () => {
            const result = getFieldTestId('field-123', 'remove');
            expect(result).toBe('workflow-field-field-123-remove');
        });

        it('should include both index and suffix in correct order', () => {
            const result = getFieldTestId('field-123', 'remove', 2);
            expect(result).toBe('workflow-field-field-123-2-remove');
        });

        it('should handle index 0', () => {
            const result = getFieldTestId('field-123', 'remove', 0);
            expect(result).toBe('workflow-field-field-123-0-remove');
        });

        it('should skip empty suffix (correct behavior)', () => {
            const result = getFieldTestId('field-123', '', 1);
            expect(result).toBe('workflow-field-field-123-1');
        });
    });
});

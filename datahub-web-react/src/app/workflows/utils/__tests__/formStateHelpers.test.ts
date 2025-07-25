import {
    type WorkflowFormState,
    applyFormFieldErrors,
    clearFormErrors,
    createInitialFormState,
    hasFormErrors,
    resetFormState,
    setFormFieldError,
    updateFormFieldValue,
} from '@app/workflows/utils/formStateHelpers';

import { ActionWorkflowField, ActionWorkflowFieldValueType, PropertyCardinality } from '@types';

describe('formStateHelpers', () => {
    const createMockField = (overrides: Partial<ActionWorkflowField> = {}): ActionWorkflowField => ({
        id: 'test-field',
        name: 'Test Field',
        valueType: ActionWorkflowFieldValueType.String,
        cardinality: PropertyCardinality.Single,
        required: true,
        ...overrides,
    });

    describe('createInitialFormState', () => {
        it('should create initial state for single cardinality fields', () => {
            const fields = [createMockField({ id: 'single-field', cardinality: PropertyCardinality.Single })];

            const state = createInitialFormState(fields);

            expect(state).toEqual({
                'single-field': {
                    id: 'single-field',
                    values: [null],
                    error: undefined,
                },
            });
        });

        it('should create initial state for multiple cardinality fields', () => {
            const fields = [createMockField({ id: 'multiple-field', cardinality: PropertyCardinality.Multiple })];

            const state = createInitialFormState(fields);

            expect(state).toEqual({
                'multiple-field': {
                    id: 'multiple-field',
                    values: [],
                    error: undefined,
                },
            });
        });

        it('should handle multiple fields with different cardinalities', () => {
            const fields = [
                createMockField({ id: 'single-field', cardinality: PropertyCardinality.Single }),
                createMockField({ id: 'multiple-field', cardinality: PropertyCardinality.Multiple }),
            ];

            const state = createInitialFormState(fields);

            expect(state).toEqual({
                'single-field': {
                    id: 'single-field',
                    values: [null],
                    error: undefined,
                },
                'multiple-field': {
                    id: 'multiple-field',
                    values: [],
                    error: undefined,
                },
            });
        });
    });

    describe('resetFormState', () => {
        it('should reset form state to initial values', () => {
            const fields = [createMockField({ id: 'test-field', cardinality: PropertyCardinality.Single })];

            const state = resetFormState(fields);

            expect(state).toEqual({
                'test-field': {
                    id: 'test-field',
                    values: [null],
                    error: undefined,
                },
            });
        });
    });

    describe('updateFormFieldValue', () => {
        it('should update field value and clear error', () => {
            const initialState: WorkflowFormState = {
                'test-field': {
                    id: 'test-field',
                    values: [null],
                    error: 'Some error',
                },
            };

            const updatedState = updateFormFieldValue(initialState, 'test-field', ['new-value']);

            expect(updatedState).toEqual({
                'test-field': {
                    id: 'test-field',
                    values: ['new-value'],
                    error: undefined,
                },
            });
            expect(updatedState).not.toBe(initialState); // Should be immutable
        });

        it('should preserve other fields when updating one field', () => {
            const initialState: WorkflowFormState = {
                'field-1': {
                    id: 'field-1',
                    values: ['value-1'],
                    error: undefined,
                },
                'field-2': {
                    id: 'field-2',
                    values: ['value-2'],
                    error: undefined,
                },
            };

            const updatedState = updateFormFieldValue(initialState, 'field-1', ['new-value']);

            expect(updatedState['field-1'].values).toEqual(['new-value']);
            expect(updatedState['field-2']).toBe(initialState['field-2']); // Other fields unchanged
        });
    });

    describe('setFormFieldError', () => {
        it('should set error for a field', () => {
            const initialState: WorkflowFormState = {
                'test-field': {
                    id: 'test-field',
                    values: ['value'],
                    error: undefined,
                },
            };

            const updatedState = setFormFieldError(initialState, 'test-field', 'Validation error');

            expect(updatedState).toEqual({
                'test-field': {
                    id: 'test-field',
                    values: ['value'],
                    error: 'Validation error',
                },
            });
            expect(updatedState).not.toBe(initialState); // Should be immutable
        });

        it('should preserve values when setting error', () => {
            const initialState: WorkflowFormState = {
                'test-field': {
                    id: 'test-field',
                    values: ['original-value'],
                    error: undefined,
                },
            };

            const updatedState = setFormFieldError(initialState, 'test-field', 'Error message');

            expect(updatedState['test-field'].values).toEqual(['original-value']);
            expect(updatedState['test-field'].error).toBe('Error message');
        });
    });

    describe('clearFormErrors', () => {
        it('should clear all errors from form state', () => {
            const initialState: WorkflowFormState = {
                'field-1': {
                    id: 'field-1',
                    values: ['value-1'],
                    error: 'Error 1',
                },
                'field-2': {
                    id: 'field-2',
                    values: ['value-2'],
                    error: 'Error 2',
                },
                'field-3': {
                    id: 'field-3',
                    values: ['value-3'],
                    error: undefined,
                },
            };

            const clearedState = clearFormErrors(initialState);

            expect(clearedState).toEqual({
                'field-1': {
                    id: 'field-1',
                    values: ['value-1'],
                    error: undefined,
                },
                'field-2': {
                    id: 'field-2',
                    values: ['value-2'],
                    error: undefined,
                },
                'field-3': {
                    id: 'field-3',
                    values: ['value-3'],
                    error: undefined,
                },
            });
        });

        it('should preserve values when clearing errors', () => {
            const initialState: WorkflowFormState = {
                'test-field': {
                    id: 'test-field',
                    values: ['important-value'],
                    error: 'Some error',
                },
            };

            const clearedState = clearFormErrors(initialState);

            expect(clearedState['test-field'].values).toEqual(['important-value']);
            expect(clearedState['test-field'].error).toBeUndefined();
        });
    });

    describe('applyFormFieldErrors', () => {
        it('should apply multiple field errors', () => {
            const initialState: WorkflowFormState = {
                'field-1': {
                    id: 'field-1',
                    values: ['value-1'],
                    error: undefined,
                },
                'field-2': {
                    id: 'field-2',
                    values: ['value-2'],
                    error: undefined,
                },
            };

            const fieldErrors = {
                'field-1': 'Error for field 1',
                'field-2': 'Error for field 2',
            };

            const updatedState = applyFormFieldErrors(initialState, fieldErrors);

            expect(updatedState).toEqual({
                'field-1': {
                    id: 'field-1',
                    values: ['value-1'],
                    error: 'Error for field 1',
                },
                'field-2': {
                    id: 'field-2',
                    values: ['value-2'],
                    error: 'Error for field 2',
                },
            });
        });

        it('should handle empty error object', () => {
            const initialState: WorkflowFormState = {
                'test-field': {
                    id: 'test-field',
                    values: ['value'],
                    error: undefined,
                },
            };

            const updatedState = applyFormFieldErrors(initialState, {});

            expect(updatedState).toBe(initialState); // Should return same reference if no errors
        });
    });

    describe('hasFormErrors', () => {
        it('should return true when form has errors', () => {
            const formState: WorkflowFormState = {
                'field-1': {
                    id: 'field-1',
                    values: ['value'],
                    error: undefined,
                },
                'field-2': {
                    id: 'field-2',
                    values: ['value'],
                    error: 'Some error',
                },
            };

            expect(hasFormErrors(formState)).toBe(true);
        });

        it('should return false when form has no errors', () => {
            const formState: WorkflowFormState = {
                'field-1': {
                    id: 'field-1',
                    values: ['value'],
                    error: undefined,
                },
                'field-2': {
                    id: 'field-2',
                    values: ['value'],
                    error: undefined,
                },
            };

            expect(hasFormErrors(formState)).toBe(false);
        });

        it('should return false for empty form state', () => {
            expect(hasFormErrors({})).toBe(false);
        });
    });
});

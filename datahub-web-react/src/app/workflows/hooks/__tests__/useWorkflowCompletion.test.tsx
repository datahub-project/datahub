import { act, renderHook } from '@testing-library/react-hooks';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import {
    UseWorkflowFormCompletionOptions,
    useWorkflowFormCompletion,
} from '@app/workflows/hooks/useWorkflowFormCompletion';

import {
    ActionWorkflowFieldConditionType,
    ActionWorkflowFieldValueType,
    FilterOperator,
    PropertyCardinality,
} from '@types';

// Mock antd message
vi.mock('antd', () => ({
    message: {
        success: vi.fn(),
        error: vi.fn(),
    },
}));

// Mock the GraphQL mutation
const mockCreateWorkflowRequest = vi.fn();
vi.mock('@graphql/actionWorkflow.generated', () => ({
    useCreateActionWorkflowFormRequestMutation: () => [mockCreateWorkflowRequest, { loading: false }],
}));

describe('useWorkflowCompletion', () => {
    const createMockWorkflow = (overrides: any = {}) => ({
        urn: 'urn:li:workflow:test',
        name: 'Test Workflow',
        category: 'ACCESS',
        trigger: {
            form: {
                fields: [
                    {
                        id: 'required-field',
                        name: 'Required Field',
                        valueType: ActionWorkflowFieldValueType.String,
                        cardinality: PropertyCardinality.Single,
                        required: true,
                    },
                    {
                        id: 'optional-field',
                        name: 'Optional Field',
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
                ],
            },
        },
        ...overrides,
    });

    beforeEach(() => {
        vi.clearAllMocks();
        mockCreateWorkflowRequest.mockResolvedValue({
            data: {
                createActionWorkflowFormRequest: 'urn:li:actionWorkflowRequest:123',
            },
        });
    });

    describe('initialization', () => {
        it('should initialize with correct default state', () => {
            const workflow = createMockWorkflow();
            const options: UseWorkflowFormCompletionOptions = { workflow };

            const { result } = renderHook(() => useWorkflowFormCompletion(options));

            expect(result.current.formState).toEqual({
                'required-field': {
                    id: 'required-field',
                    values: [null],
                    error: undefined,
                },
                'optional-field': {
                    id: 'optional-field',
                    values: [null],
                    error: undefined,
                },
                'multiple-field': {
                    id: 'multiple-field',
                    values: [],
                    error: undefined,
                },
            });
            expect(result.current.description).toBe('');
            expect(result.current.expiresAt).toBeUndefined();
            expect(result.current.isSubmitting).toBe(false);
        });

        it('should handle multiple cardinality fields correctly', () => {
            const workflow = createMockWorkflow({
                trigger: {
                    form: {
                        fields: [
                            {
                                id: 'multiple-field',
                                name: 'Multiple Field',
                                valueType: ActionWorkflowFieldValueType.String,
                                cardinality: PropertyCardinality.Multiple,
                                required: false,
                            },
                        ],
                    },
                },
            });

            const { result } = renderHook(() => useWorkflowFormCompletion({ workflow }));

            expect(result.current.formState['multiple-field'].values).toEqual([]);
        });
    });

    describe('form state management', () => {
        it('should update field values correctly', () => {
            const workflow = createMockWorkflow();
            const { result } = renderHook(() => useWorkflowFormCompletion({ workflow }));

            act(() => {
                result.current.updateFieldValue('required-field', ['test-value']);
            });

            expect(result.current.formState['required-field'].values).toEqual(['test-value']);
            expect(result.current.formState['required-field'].error).toBeUndefined();
        });

        it('should set field errors correctly', () => {
            const workflow = createMockWorkflow();
            const { result } = renderHook(() => useWorkflowFormCompletion({ workflow }));

            act(() => {
                result.current.setFieldError('required-field', 'Test error');
            });

            expect(result.current.formState['required-field'].error).toBe('Test error');
        });

        it('should clear errors when updating field values', () => {
            const workflow = createMockWorkflow();
            const { result } = renderHook(() => useWorkflowFormCompletion({ workflow }));

            // Set an error first
            act(() => {
                result.current.setFieldError('required-field', 'Test error');
            });

            expect(result.current.formState['required-field'].error).toBe('Test error');

            // Update the field value - should clear the error
            act(() => {
                result.current.updateFieldValue('required-field', ['new-value']);
            });

            expect(result.current.formState['required-field'].error).toBeUndefined();
        });

        it('should update description and expiresAt', () => {
            const workflow = createMockWorkflow();
            const { result } = renderHook(() => useWorkflowFormCompletion({ workflow }));

            act(() => {
                result.current.setDescription('Test description');
                result.current.setExpiresAt(1234567890);
            });

            expect(result.current.description).toBe('Test description');
            expect(result.current.expiresAt).toBe(1234567890);
        });
    });

    describe('form validation', () => {
        it('should validate required fields', () => {
            const workflow = createMockWorkflow();
            const { result } = renderHook(() => useWorkflowFormCompletion({ workflow }));

            // Initially invalid - required field is empty
            expect(result.current.isValid()).toBe(false);

            // Fill required field
            act(() => {
                result.current.updateFieldValue('required-field', ['test-value']);
            });

            expect(result.current.isValid()).toBe(true);
        });

        it('should validate field types', () => {
            const workflow = createMockWorkflow();
            const { result } = renderHook(() => useWorkflowFormCompletion({ workflow }));

            // Set invalid type for number field
            act(() => {
                result.current.updateFieldValue('required-field', ['valid-string']);
                result.current.updateFieldValue('optional-field', ['not-a-number']);
            });

            expect(result.current.isValid()).toBe(false);
            expect(result.current.formState['optional-field'].error).toContain('Invalid value type');
        });

        it('should set validation errors on form state', () => {
            const workflow = createMockWorkflow();
            const { result } = renderHook(() => useWorkflowFormCompletion({ workflow }));

            // Try to validate with empty required field
            act(() => {
                result.current.isValid();
            });

            expect(result.current.formState['required-field'].error).toContain('is required');
        });
    });

    describe('form reset', () => {
        it('should reset form to initial state', () => {
            const workflow = createMockWorkflow();
            const { result } = renderHook(() => useWorkflowFormCompletion({ workflow }));

            // Modify form state
            act(() => {
                result.current.updateFieldValue('required-field', ['test-value']);
                result.current.setDescription('Test description');
                result.current.setExpiresAt(1234567890);
            });

            expect(result.current.formState['required-field'].values).toEqual(['test-value']);

            // Reset form
            act(() => {
                result.current.resetForm();
            });

            expect(result.current.formState['required-field'].values).toEqual([null]);
            expect(result.current.description).toBe('');
            expect(result.current.expiresAt).toBeUndefined();
        });
    });

    describe('workflow submission', () => {
        it('should submit workflow successfully', async () => {
            const workflow = createMockWorkflow();
            const onSuccess = vi.fn();

            const { result } = renderHook(() => useWorkflowFormCompletion({ workflow, onSuccess }));

            // Fill required field
            act(() => {
                result.current.updateFieldValue('required-field', ['test-value']);
            });

            // Submit workflow
            await act(async () => {
                await result.current.submitWorkflow();
            });

            expect(onSuccess).toHaveBeenCalledWith('urn:li:actionWorkflowRequest:123');
        });

        it('should handle submission errors', async () => {
            const workflow = createMockWorkflow();
            const onError = vi.fn();

            mockCreateWorkflowRequest.mockRejectedValue(new Error('GraphQL error'));

            const { result } = renderHook(() => useWorkflowFormCompletion({ workflow, onError }));

            act(() => {
                result.current.updateFieldValue('required-field', ['test-value']);
            });

            await act(async () => {
                await result.current.submitWorkflow();
            });

            expect(onError).toHaveBeenCalledWith(expect.any(Error));
        });

        it('should not submit if form validation fails', async () => {
            const workflow = createMockWorkflow();
            const onSuccess = vi.fn();

            const { result } = renderHook(() => useWorkflowFormCompletion({ workflow, onSuccess }));

            // Try to submit without filling required field
            await act(async () => {
                await result.current.submitWorkflow();
            });

            expect(onSuccess).not.toHaveBeenCalled();
            expect(result.current.formState['required-field'].error).toContain('is required');
        });
    });

    describe('conditional fields', () => {
        it('should skip validation for hidden conditional fields', () => {
            const workflow = createMockWorkflow({
                trigger: {
                    form: {
                        fields: [
                            {
                                id: 'trigger-field',
                                name: 'Trigger Field',
                                valueType: ActionWorkflowFieldValueType.String,
                                cardinality: PropertyCardinality.Single,
                                required: false,
                            },
                            {
                                id: 'conditional-field',
                                name: 'Conditional Field',
                                valueType: ActionWorkflowFieldValueType.String,
                                cardinality: PropertyCardinality.Single,
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
                            },
                        ],
                    },
                },
            });

            const { result } = renderHook(() => useWorkflowFormCompletion({ workflow }));

            // Set trigger field to hide conditional field
            act(() => {
                result.current.updateFieldValue('trigger-field', ['HIDE']);
            });

            // Should be valid even though conditional field is empty and required
            expect(result.current.isValid()).toBe(true);

            // Set trigger field to show conditional field
            act(() => {
                result.current.updateFieldValue('trigger-field', ['SHOW']);
            });

            // Should now be invalid because conditional field is required but empty
            expect(result.current.isValid()).toBe(false);
        });
    });
});

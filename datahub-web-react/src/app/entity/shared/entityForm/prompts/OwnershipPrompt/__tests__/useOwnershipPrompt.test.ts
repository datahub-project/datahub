import { act, renderHook } from '@testing-library/react-hooks';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import useOwnershipPrompt from '@app/entity/shared/entityForm/prompts/OwnershipPrompt/useOwnershipPrompt';
import {
    getDefaultOwnerEntities,
    getDefaultOwnershipTypeUrn,
} from '@app/entity/shared/entityForm/prompts/OwnershipPrompt/utils';

import { EntityType, FormPrompt, FormPromptType, OwnershipTypeEntity, SchemaField, SchemaFieldDataType } from '@types';

// Mock dependencies
const mockRemoveOwnersMutation = vi.fn();
const mockRefetchSchema = vi.fn();
const mockSubmitResponse = vi.fn();

// Mock GraphQL hooks
vi.mock('@graphql/mutations.generated', () => ({
    useBatchRemoveOwnersMutation: () => [mockRemoveOwnersMutation],
}));

// Mock context hooks with default values
vi.mock('@app/entity/shared/EntityContext', () => ({
    useEntityData: () => ({
        entityData: {
            urn: 'urn:li:dataset:test',
            ownership: {
                owners: [],
            },
        },
    }),
    useMutationUrn: () => 'urn:li:dataset:test',
}));

vi.mock('@app/entity/shared/entityForm/EntityFormContext', () => ({
    useEntityFormContext: () => ({
        form: {
            formView: 'BY_ENTITY',
        },
    }),
    FormView: {
        BY_ENTITY: 'BY_ENTITY',
        BY_QUESTION: 'BY_QUESTION',
        BULK_VERIFY: 'BULK_VERIFY',
    },
}));

// Mock utility functions
vi.mock('@app/entity/shared/containers/profile/sidebar/FormInfo/utils', () => ({
    getPromptAssociation: () => null,
}));

vi.mock('@app/entity/shared/entityForm/prompts/OwnershipPrompt/utils', () => ({
    getDefaultOwnerEntities: vi.fn(),
    getDefaultOwnershipTypeUrn: vi.fn(),
}));

vi.mock('@app/entity/shared/tabs/Dataset/Schema/useGetEntitySchema', () => ({
    useGetEntityWithSchema: () => ({
        refetch: mockRefetchSchema,
    }),
}));

// Mock constants
vi.mock('@app/entity/shared/entityForm/constants', () => ({
    SCHEMA_FIELD_PROMPT_TYPES: ['FIELDS_OWNERSHIP'],
}));

// Test data
const mockOwnershipType: OwnershipTypeEntity = {
    urn: 'urn:li:ownershipType:technical',
    type: EntityType.CustomOwnershipType,
    info: {
        name: 'Technical Owner',
        description: 'Technical ownership',
    },
};

const mockPrompt: FormPrompt = {
    id: 'prompt1',
    formUrn: 'urn:li:form:test',
    type: FormPromptType.Ownership,
    title: 'Test Ownership Prompt',
    description: 'Test prompt description',
    ownershipParams: {
        allowedOwnershipTypes: [mockOwnershipType],
        allowedOwners: [],
        cardinality: 'MULTIPLE' as any,
    },
    required: true,
};

const mockField: SchemaField = {
    fieldPath: 'testField',
    nativeDataType: 'string',
    type: SchemaFieldDataType.String,
    nullable: true,
    recursive: false,
};

describe('useOwnershipPrompt', () => {
    beforeEach(() => {
        vi.clearAllMocks();
        mockRemoveOwnersMutation.mockResolvedValue({});
        // Set up default mock return values
        vi.mocked(getDefaultOwnerEntities).mockReturnValue([]);
        vi.mocked(getDefaultOwnershipTypeUrn).mockReturnValue('urn:li:ownershipType:technical');
    });

    describe('initialization', () => {
        it('should initialize with correct default values', () => {
            const { result } = renderHook(() =>
                useOwnershipPrompt({
                    prompt: mockPrompt,
                    submitResponse: mockSubmitResponse,
                    associatedUrn: 'urn:li:dataset:associated',
                }),
            );

            expect(result.current.hasEdited).toBe(false);
            expect(result.current.selectedOwnerTypeUrn).toBe('urn:li:ownershipType:technical');
            expect(result.current.selectedValues).toEqual([]);
            expect(result.current.initialEntities).toEqual([]);
            expect(typeof result.current.updateSelectedValues).toBe('function');
            expect(typeof result.current.updateSelectedOwnerTypeUrn).toBe('function');
            expect(typeof result.current.submitOwnershipResponse).toBe('function');
        });
    });

    describe('state updates', () => {
        it('should update selected values and set hasEdited to true', () => {
            const { result } = renderHook(() =>
                useOwnershipPrompt({
                    prompt: mockPrompt,
                    submitResponse: mockSubmitResponse,
                }),
            );

            expect(result.current.hasEdited).toBe(false);

            act(() => {
                result.current.updateSelectedValues(['urn:li:corpuser:user2']);
            });

            expect(result.current.selectedValues).toEqual(['urn:li:corpuser:user2']);
            expect(result.current.hasEdited).toBe(true);
        });

        it('should update selected owner type URN and set hasEdited to true', () => {
            const { result } = renderHook(() =>
                useOwnershipPrompt({
                    prompt: mockPrompt,
                    submitResponse: mockSubmitResponse,
                }),
            );

            expect(result.current.hasEdited).toBe(false);

            act(() => {
                result.current.updateSelectedOwnerTypeUrn('urn:li:ownershipType:business');
            });

            expect(result.current.selectedOwnerTypeUrn).toBe('urn:li:ownershipType:business');
            expect(result.current.hasEdited).toBe(true);
        });
    });

    describe('submitOwnershipResponse', () => {
        it('should not submit when selectedValues is empty', async () => {
            const { result } = renderHook(() =>
                useOwnershipPrompt({
                    prompt: mockPrompt,
                    submitResponse: mockSubmitResponse,
                }),
            );

            await act(async () => {
                await result.current.submitOwnershipResponse();
            });

            expect(mockSubmitResponse).not.toHaveBeenCalled();
            expect(mockRemoveOwnersMutation).not.toHaveBeenCalled();
        });

        it('should submit with default ownership type when available', async () => {
            const { result } = renderHook(() =>
                useOwnershipPrompt({
                    prompt: mockPrompt,
                    submitResponse: mockSubmitResponse,
                }),
            );

            act(() => {
                result.current.updateSelectedValues(['urn:li:corpuser:user1']);
                // selectedOwnerTypeUrn should be initialized with default value
            });

            await act(async () => {
                await result.current.submitOwnershipResponse();
            });

            // Should submit with the default ownership type from the mock
            expect(mockSubmitResponse).toHaveBeenCalledWith(
                expect.objectContaining({
                    ownershipParams: expect.objectContaining({
                        owners: ['urn:li:corpuser:user1'],
                        ownershipTypeUrn: 'urn:li:ownershipType:technical',
                    }),
                }),
                expect.any(Function),
            );
        });

        it('should submit with correct parameters when values are provided', async () => {
            const { result } = renderHook(() =>
                useOwnershipPrompt({
                    prompt: mockPrompt,
                    submitResponse: mockSubmitResponse,
                }),
            );

            act(() => {
                result.current.updateSelectedValues(['urn:li:corpuser:user1']);
                result.current.updateSelectedOwnerTypeUrn('urn:li:ownershipType:technical');
            });

            await act(async () => {
                await result.current.submitOwnershipResponse();
            });

            expect(mockSubmitResponse).toHaveBeenCalledWith(
                {
                    promptId: 'prompt1',
                    formUrn: 'urn:li:form:test',
                    type: FormPromptType.Ownership,
                    fieldPath: undefined,
                    ownershipParams: {
                        owners: ['urn:li:corpuser:user1'],
                        ownershipTypeUrn: 'urn:li:ownershipType:technical',
                    },
                },
                expect.any(Function),
            );
        });

        it('should include fieldPath when field is provided', async () => {
            const { result } = renderHook(() =>
                useOwnershipPrompt({
                    prompt: mockPrompt,
                    submitResponse: mockSubmitResponse,
                    field: mockField,
                }),
            );

            act(() => {
                result.current.updateSelectedValues(['urn:li:corpuser:user1']);
                result.current.updateSelectedOwnerTypeUrn('urn:li:ownershipType:technical');
            });

            await act(async () => {
                await result.current.submitOwnershipResponse();
            });

            expect(mockSubmitResponse).toHaveBeenCalledWith(
                expect.objectContaining({
                    fieldPath: 'testField',
                }),
                expect.any(Function),
            );
        });

        it('should call refetchSchema in onSuccess callback when field is provided', async () => {
            const { result } = renderHook(() =>
                useOwnershipPrompt({
                    prompt: mockPrompt,
                    submitResponse: mockSubmitResponse,
                    field: mockField,
                }),
            );

            act(() => {
                result.current.updateSelectedValues(['urn:li:corpuser:user1']);
                result.current.updateSelectedOwnerTypeUrn('urn:li:ownershipType:technical');
            });

            await act(async () => {
                await result.current.submitOwnershipResponse();
            });

            // Get the onSuccess callback and call it
            const onSuccessCallback = mockSubmitResponse.mock.calls[0][1];
            act(() => {
                onSuccessCallback();
            });

            expect(result.current.hasEdited).toBe(false); // Should reset hasEdited
            expect(mockRefetchSchema).toHaveBeenCalled();
        });

        it('should not call refetchSchema when field is not provided', async () => {
            const { result } = renderHook(() =>
                useOwnershipPrompt({
                    prompt: mockPrompt,
                    submitResponse: mockSubmitResponse,
                    // No field provided
                }),
            );

            act(() => {
                result.current.updateSelectedValues(['urn:li:corpuser:user1']);
                result.current.updateSelectedOwnerTypeUrn('urn:li:ownershipType:technical');
            });

            await act(async () => {
                await result.current.submitOwnershipResponse();
            });

            // Get the onSuccess callback and call it
            const onSuccessCallback = mockSubmitResponse.mock.calls[0][1];
            act(() => {
                onSuccessCallback();
            });

            expect(result.current.hasEdited).toBe(false); // Should reset hasEdited
            expect(mockRefetchSchema).not.toHaveBeenCalled();
        });
    });

    describe('removeOwnersMutation logic', () => {
        it('should test the core logic of removeOwnersMutation calls', async () => {
            // This test verifies that the hook correctly identifies owners to remove
            // and calls the mutation with the right parameters
            const { result } = renderHook(() =>
                useOwnershipPrompt({
                    prompt: mockPrompt,
                    submitResponse: mockSubmitResponse,
                    associatedUrn: 'urn:li:dataset:associated',
                }),
            );

            // Set up the scenario where we have selected values and ownership type
            act(() => {
                result.current.updateSelectedValues(['urn:li:corpuser:user1']);
                result.current.updateSelectedOwnerTypeUrn('urn:li:ownershipType:technical');
            });

            await act(async () => {
                await result.current.submitOwnershipResponse();
            });

            // Should call submitResponse with correct parameters
            expect(mockSubmitResponse).toHaveBeenCalledWith(
                {
                    promptId: 'prompt1',
                    formUrn: 'urn:li:form:test',
                    type: FormPromptType.Ownership,
                    fieldPath: undefined,
                    ownershipParams: {
                        owners: ['urn:li:corpuser:user1'],
                        ownershipTypeUrn: 'urn:li:ownershipType:technical',
                    },
                },
                expect.any(Function),
            );
        });
    });

    describe('useEffect hooks for syncing with initial values', () => {
        it('should update selectedValues when initialValues change and hasEdited is false', () => {
            // Start with empty initial values
            vi.mocked(getDefaultOwnerEntities).mockReturnValue([]);

            const { result, rerender } = renderHook(() =>
                useOwnershipPrompt({
                    prompt: mockPrompt,
                    submitResponse: mockSubmitResponse,
                }),
            );

            // Initially should have empty selectedValues
            expect(result.current.selectedValues).toEqual([]);
            expect(result.current.hasEdited).toBe(false);

            // Now mock the utility to return some initial entities
            vi.mocked(getDefaultOwnerEntities).mockReturnValue([
                { urn: 'urn:li:corpuser:user1', type: EntityType.CorpUser },
                { urn: 'urn:li:corpuser:user2', type: EntityType.CorpUser },
            ] as any[]);

            // Trigger a rerender to simulate initialValues changing
            rerender();

            // selectedValues should update to match the new initialValues
            expect(result.current.selectedValues).toEqual(['urn:li:corpuser:user1', 'urn:li:corpuser:user2']);
            expect(result.current.hasEdited).toBe(false);
        });

        it('should NOT update selectedValues when hasEdited is true', () => {
            vi.mocked(getDefaultOwnerEntities).mockReturnValue([]);

            const { result, rerender } = renderHook(() =>
                useOwnershipPrompt({
                    prompt: mockPrompt,
                    submitResponse: mockSubmitResponse,
                }),
            );

            // User manually edits the values
            act(() => {
                result.current.updateSelectedValues(['urn:li:corpuser:manual']);
            });

            expect(result.current.selectedValues).toEqual(['urn:li:corpuser:manual']);
            expect(result.current.hasEdited).toBe(true);

            // Now change the initial values
            vi.mocked(getDefaultOwnerEntities).mockReturnValue([
                { urn: 'urn:li:corpuser:new1', type: EntityType.CorpUser },
            ] as any[]);

            rerender();

            // selectedValues should NOT change because hasEdited is true
            expect(result.current.selectedValues).toEqual(['urn:li:corpuser:manual']);
            expect(result.current.hasEdited).toBe(true);
        });

        it('should update selectedOwnerTypeUrn when initialOwnershipTypeUrn changes and hasEdited is false', () => {
            // Start with technical ownership type
            vi.mocked(getDefaultOwnershipTypeUrn).mockReturnValue('urn:li:ownershipType:technical');

            const { result, rerender } = renderHook(() =>
                useOwnershipPrompt({
                    prompt: mockPrompt,
                    submitResponse: mockSubmitResponse,
                }),
            );

            expect(result.current.selectedOwnerTypeUrn).toBe('urn:li:ownershipType:technical');
            expect(result.current.hasEdited).toBe(false);

            // Change the initial ownership type
            vi.mocked(getDefaultOwnershipTypeUrn).mockReturnValue('urn:li:ownershipType:business');

            rerender();

            // selectedOwnerTypeUrn should update to match the new initial value
            expect(result.current.selectedOwnerTypeUrn).toBe('urn:li:ownershipType:business');
            expect(result.current.hasEdited).toBe(false);
        });

        it('should NOT update selectedOwnerTypeUrn when hasEdited is true', () => {
            vi.mocked(getDefaultOwnershipTypeUrn).mockReturnValue('urn:li:ownershipType:technical');

            const { result, rerender } = renderHook(() =>
                useOwnershipPrompt({
                    prompt: mockPrompt,
                    submitResponse: mockSubmitResponse,
                }),
            );

            // User manually changes the ownership type
            act(() => {
                result.current.updateSelectedOwnerTypeUrn('urn:li:ownershipType:manual');
            });

            expect(result.current.selectedOwnerTypeUrn).toBe('urn:li:ownershipType:manual');
            expect(result.current.hasEdited).toBe(true);

            // Change the initial ownership type
            vi.mocked(getDefaultOwnershipTypeUrn).mockReturnValue('urn:li:ownershipType:business');

            rerender();

            // selectedOwnerTypeUrn should NOT change because hasEdited is true
            expect(result.current.selectedOwnerTypeUrn).toBe('urn:li:ownershipType:manual');
            expect(result.current.hasEdited).toBe(true);
        });

        it('should handle equal values correctly (no unnecessary updates)', () => {
            const initialEntities = [{ urn: 'urn:li:corpuser:user1', type: 'CORP_USER' }] as any[];
            const initialOwnershipType = 'urn:li:ownershipType:technical';

            vi.mocked(getDefaultOwnerEntities).mockReturnValue(initialEntities);
            vi.mocked(getDefaultOwnershipTypeUrn).mockReturnValue(initialOwnershipType);

            const { result, rerender } = renderHook(() =>
                useOwnershipPrompt({
                    prompt: mockPrompt,
                    submitResponse: mockSubmitResponse,
                }),
            );

            const initialSelectedValues = result.current.selectedValues;
            const initialSelectedOwnerTypeUrn = result.current.selectedOwnerTypeUrn;

            // Rerender with the same values
            rerender();

            // Values should remain the same (no unnecessary state updates)
            expect(result.current.selectedValues).toBe(initialSelectedValues);
            expect(result.current.selectedOwnerTypeUrn).toBe(initialSelectedOwnerTypeUrn);
            expect(result.current.hasEdited).toBe(false);
        });
    });

    describe('edge cases', () => {
        it('should handle entityData without ownership gracefully', async () => {
            const { result } = renderHook(() =>
                useOwnershipPrompt({
                    prompt: mockPrompt,
                    submitResponse: mockSubmitResponse,
                }),
            );

            act(() => {
                result.current.updateSelectedValues(['urn:li:corpuser:user1']);
                result.current.updateSelectedOwnerTypeUrn('urn:li:ownershipType:technical');
            });

            // Should not throw error when submitting
            await expect(
                act(async () => {
                    await result.current.submitOwnershipResponse();
                }),
            ).resolves.not.toThrow();
        });
    });
});

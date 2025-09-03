import { act, renderHook } from '@testing-library/react-hooks';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import useRemoveSuggested from '@app/entity/shared/entityForm/prompts/useRemoveSuggested';

import { FormPromptType, SubResourceType, SubmitFormPromptInput } from '@types';

// Mock GraphQL mutations
const removeTermsMutationMock = vi.fn();
const removeOwnersMutationMock = vi.fn();

vi.mock('@graphql/mutations.generated', () => ({
    useBatchRemoveTermsMutation: () => [removeTermsMutationMock],
    useBatchRemoveOwnersMutation: () => [removeOwnersMutationMock],
}));

describe('useRemoveSuggested', () => {
    const associatedUrn = 'urn:li:dataset:1';

    // Base form input
    const baseFormInput: SubmitFormPromptInput = {
        formUrn: 'urn:li:form1',
        promptId: '1',
        type: FormPromptType.GlossaryTerms,
        fieldPath: '',
        fieldPaths: [],
    };

    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('should not call remove terms mutation when urnsToRemove is empty', async () => {
        const { result } = renderHook(() => useRemoveSuggested([], FormPromptType.GlossaryTerms, associatedUrn));
        await act(async () => {
            await result.current.removeInitialSuggested(baseFormInput);
        });
        expect(removeTermsMutationMock).not.toHaveBeenCalled();
    });

    it('should call remove terms mutation with deduplication', async () => {
        removeTermsMutationMock.mockResolvedValueOnce({});
        const { result } = renderHook(() =>
            useRemoveSuggested(['urn:term:1', 'urn:term:2', 'urn:term:1'], FormPromptType.GlossaryTerms, associatedUrn),
        );
        await act(async () => {
            await result.current.removeInitialSuggested(baseFormInput);
        });
        expect(removeTermsMutationMock).toHaveBeenCalledWith({
            variables: {
                input: {
                    termUrns: ['urn:term:1', 'urn:term:2'],
                    resources: [{ resourceUrn: associatedUrn }],
                },
            },
        });
        expect(removeOwnersMutationMock).not.toHaveBeenCalled();
    });

    it('should not call owners mutation when urnsToRemove is empty', async () => {
        const { result } = renderHook(() => useRemoveSuggested([], FormPromptType.Ownership, associatedUrn));
        await act(async () => {
            await result.current.removeInitialSuggested({
                ...baseFormInput,
                type: FormPromptType.Ownership,
            });
        });
        expect(removeOwnersMutationMock).not.toHaveBeenCalled();
    });

    it('should call remove owners mutation with deduplication', async () => {
        removeOwnersMutationMock.mockResolvedValueOnce({});
        const { result } = renderHook(() =>
            useRemoveSuggested(['urn:own1', 'urn:own1', 'urn:own2'], FormPromptType.Ownership, associatedUrn),
        );
        await act(async () => {
            await result.current.removeInitialSuggested({
                ...baseFormInput,
                type: FormPromptType.Ownership,
            });
        });
        expect(removeOwnersMutationMock).toHaveBeenCalledWith({
            variables: {
                input: {
                    ownerUrns: ['urn:own1', 'urn:own2'],
                    resources: [{ resourceUrn: associatedUrn }],
                },
            },
        });
    });

    it('should call remove terms mutation for FieldsGlossaryTerms prompt type when fieldPath provided', async () => {
        removeTermsMutationMock.mockResolvedValueOnce({});
        const { result } = renderHook(() =>
            useRemoveSuggested(['urn:t3'], FormPromptType.FieldsGlossaryTerms, associatedUrn),
        );
        await act(async () => {
            await result.current.removeInitialSuggested({
                ...baseFormInput,
                type: FormPromptType.FieldsGlossaryTerms,
                fieldPath: 'col1',
            });
        });
        expect(removeTermsMutationMock).toHaveBeenCalledWith({
            variables: {
                input: {
                    termUrns: ['urn:t3'],
                    resources: [
                        {
                            resourceUrn: associatedUrn,
                            subResource: 'col1',
                            subResourceType: SubResourceType.DatasetField,
                        },
                    ],
                },
            },
        });
    });

    it('should not call remove terms mutation for FieldsGlossaryTerms prompt type when fieldPath is missing, empty, null, or undefined', async () => {
        const falsyPaths: (undefined | null | '')[] = [undefined, null, ''];
        await Promise.all(
            falsyPaths.map(async (path) => {
                const { result } = renderHook(() =>
                    useRemoveSuggested(['urn:t4'], FormPromptType.FieldsGlossaryTerms, associatedUrn),
                );
                await act(async () => {
                    await result.current.removeInitialSuggested({
                        ...baseFormInput,
                        type: FormPromptType.FieldsGlossaryTerms,
                        fieldPath: path as any,
                    });
                });
                expect(removeTermsMutationMock).not.toHaveBeenCalled();
            }),
        );
    });

    it('should not call any mutation for unknown prompt type', async () => {
        const { result } = renderHook(() =>
            useRemoveSuggested(['urn:x'], 'UnknownPromptType' as FormPromptType, associatedUrn),
        );
        await act(async () => {
            await result.current.removeInitialSuggested(baseFormInput);
        });
        expect(removeTermsMutationMock).not.toHaveBeenCalled();
        expect(removeOwnersMutationMock).not.toHaveBeenCalled();
    });
});

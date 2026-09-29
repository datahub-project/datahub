import { waitFor } from '@testing-library/react';
import { act, renderHook } from '@testing-library/react-hooks';
import React from 'react';

import { useUserContext } from '@app/context/useUserContext';
import { EducationStepsContext } from '@providers/EducationStepsContext';
import useUpdateEducationStep from '@providers/hooks/useUpdateEducationStep';

import { useBatchUpdateStepStatesMutation } from '@graphql/step.generated';
import { StepStateResult } from '@types';

// Mock dependencies
vi.mock('@app/context/useUserContext', () => ({
    useUserContext: vi.fn(),
}));

vi.mock('@app/onboarding/utils', () => ({
    convertStepId: vi.fn((stepId: string, userUrn: string) => `${userUrn}-${stepId}`),
}));

vi.mock('@graphql/step.generated', () => ({
    useBatchUpdateStepStatesMutation: vi.fn(),
}));

const mockUseUserContext = useUserContext as ReturnType<typeof vi.fn>;
const mockUseBatchUpdateStepStatesMutation = useBatchUpdateStepStatesMutation as ReturnType<typeof vi.fn>;

describe('useUpdateEducationStep', () => {
    const testUserUrn = 'urn:li:corpuser:test-user';
    const testStepId = 'test-step';
    let mockSetEducationSteps: ReturnType<typeof vi.fn>;
    let mockBatchUpdateStepStates: ReturnType<typeof vi.fn>;

    beforeEach(() => {
        vi.clearAllMocks();
        mockSetEducationSteps = vi.fn();
        mockBatchUpdateStepStates = vi.fn(() => Promise.resolve({ data: {} }));

        mockUseBatchUpdateStepStatesMutation.mockReturnValue([mockBatchUpdateStepStates]);
    });

    describe('updateEducationStep for user', () => {
        beforeEach(() => {
            mockUseUserContext.mockReturnValue({ urn: testUserUrn });
        });

        it('should call mutation with converted stepId when isForUser=true', async () => {
            const educationSteps: StepStateResult[] = [];
            const wrapper = ({ children }: { children: React.ReactNode }) => (
                <EducationStepsContext.Provider
                    value={{
                        educationSteps,
                        setEducationSteps: mockSetEducationSteps,
                        educationStepIdsAllowlist: new Set(),
                        setEducationStepIdsAllowlist: vi.fn(),
                    }}
                >
                    {children}
                </EducationStepsContext.Provider>
            );

            const { result } = renderHook(() => useUpdateEducationStep(), { wrapper });

            await act(async () => {
                result.current.updateEducationStep(testStepId, true);
            });

            await waitFor(() => {
                expect(mockBatchUpdateStepStates).toHaveBeenCalledWith({
                    variables: {
                        input: {
                            states: [{ id: `${testUserUrn}-${testStepId}`, properties: [] }],
                        },
                    },
                });
            });
        });

        it('should update educationSteps after successful mutation', async () => {
            const existingSteps: StepStateResult[] = [{ id: 'existing-step', properties: [] }];
            const wrapper = ({ children }: { children: React.ReactNode }) => (
                <EducationStepsContext.Provider
                    value={{
                        educationSteps: existingSteps,
                        setEducationSteps: mockSetEducationSteps,
                        educationStepIdsAllowlist: new Set(),
                        setEducationStepIdsAllowlist: vi.fn(),
                    }}
                >
                    {children}
                </EducationStepsContext.Provider>
            );

            const { result } = renderHook(() => useUpdateEducationStep(), { wrapper });

            await act(async () => {
                result.current.updateEducationStep(testStepId, true);
            });

            await waitFor(() => {
                expect(mockSetEducationSteps).toHaveBeenCalled();
            });

            // Verify the updater function adds the new step
            const updaterFunction = mockSetEducationSteps.mock.calls[0][0];
            const updatedSteps = updaterFunction(existingSteps);
            expect(updatedSteps).toHaveLength(2);
            expect(updatedSteps[1]).toEqual({
                id: `${testUserUrn}-${testStepId}`,
                properties: [],
            });
        });

        it('should initialize educationSteps array when null', async () => {
            const wrapper = ({ children }: { children: React.ReactNode }) => (
                <EducationStepsContext.Provider
                    value={{
                        educationSteps: null,
                        setEducationSteps: mockSetEducationSteps,
                        educationStepIdsAllowlist: new Set(),
                        setEducationStepIdsAllowlist: vi.fn(),
                    }}
                >
                    {children}
                </EducationStepsContext.Provider>
            );

            const { result } = renderHook(() => useUpdateEducationStep(), { wrapper });

            await act(async () => {
                result.current.updateEducationStep(testStepId, true);
            });

            await waitFor(() => {
                expect(mockSetEducationSteps).toHaveBeenCalled();
            });

            // Verify the updater function creates a new array when existing is null
            const updaterFunction = mockSetEducationSteps.mock.calls[0][0];
            const updatedSteps = updaterFunction(null);
            expect(updatedSteps).toEqual([
                {
                    id: `${testUserUrn}-${testStepId}`,
                    properties: [],
                },
            ]);
        });
    });

    describe('updateEducationStep for non-user steps', () => {
        beforeEach(() => {
            mockUseUserContext.mockReturnValue({ urn: testUserUrn });
        });

        it('should use non-converted stepId when isForUser=false', async () => {
            const educationSteps: StepStateResult[] = [];
            const wrapper = ({ children }: { children: React.ReactNode }) => (
                <EducationStepsContext.Provider
                    value={{
                        educationSteps,
                        setEducationSteps: mockSetEducationSteps,
                        educationStepIdsAllowlist: new Set(),
                        setEducationStepIdsAllowlist: vi.fn(),
                    }}
                >
                    {children}
                </EducationStepsContext.Provider>
            );

            const { result } = renderHook(() => useUpdateEducationStep(), { wrapper });

            await act(async () => {
                result.current.updateEducationStep(testStepId, false);
            });

            await waitFor(() => {
                expect(mockBatchUpdateStepStates).toHaveBeenCalledWith({
                    variables: {
                        input: {
                            states: [{ id: testStepId, properties: [] }],
                        },
                    },
                });
            });
        });
    });

    describe('updateEducationStep with no user', () => {
        it('should handle empty user urn gracefully', async () => {
            mockUseUserContext.mockReturnValue({ urn: null });

            const educationSteps: StepStateResult[] = [];
            const wrapper = ({ children }: { children: React.ReactNode }) => (
                <EducationStepsContext.Provider
                    value={{
                        educationSteps,
                        setEducationSteps: mockSetEducationSteps,
                        educationStepIdsAllowlist: new Set(),
                        setEducationStepIdsAllowlist: vi.fn(),
                    }}
                >
                    {children}
                </EducationStepsContext.Provider>
            );

            const { result } = renderHook(() => useUpdateEducationStep(), { wrapper });

            await act(async () => {
                result.current.updateEducationStep(testStepId, true);
            });

            await waitFor(() => {
                expect(mockBatchUpdateStepStates).toHaveBeenCalledWith({
                    variables: {
                        input: {
                            states: [{ id: `-${testStepId}`, properties: [] }],
                        },
                    },
                });
            });
        });
    });
});

import { renderHook } from '@testing-library/react-hooks';
import React from 'react';

import { useUserContext } from '@app/context/useUserContext';
import { EducationStepsContext } from '@providers/EducationStepsContext';
import useHasSeenEducationStep from '@providers/hooks/useHasSeenEducationStep';

import { StepStateResult } from '@types';

// Mock dependencies
vi.mock('@app/context/useUserContext', () => ({
    useUserContext: vi.fn(),
}));

vi.mock('@app/onboarding/utils', () => ({
    convertStepId: vi.fn((stepId: string, userUrn: string) => `${userUrn}-${stepId}`),
}));

const mockUseUserContext = useUserContext as ReturnType<typeof vi.fn>;

describe('useHasSeenEducationStep', () => {
    const mockSetEducationSteps = vi.fn();
    const mockSetEducationStepIdsAllowlist = vi.fn();
    const testUserUrn = 'urn:li:corpuser:test-user';
    const testStepId = 'test-step';

    beforeEach(() => {
        vi.clearAllMocks();
    });

    describe('when user is not loaded', () => {
        beforeEach(() => {
            mockUseUserContext.mockReturnValue({ user: null });
        });

        it('should return true when isForUser=true and user is not loaded', () => {
            const educationSteps: StepStateResult[] = [];
            const wrapper = ({ children }: { children: React.ReactNode }) => (
                <EducationStepsContext.Provider
                    value={{
                        educationSteps,
                        setEducationSteps: mockSetEducationSteps,
                        educationStepIdsAllowlist: new Set(),
                        setEducationStepIdsAllowlist: mockSetEducationStepIdsAllowlist,
                    }}
                >
                    {children}
                </EducationStepsContext.Provider>
            );

            const { result } = renderHook(() => useHasSeenEducationStep(testStepId, true), { wrapper });

            expect(result.current).toBe(true);
        });

        it('should check for non-user-specific step when isForUser=false', () => {
            const educationSteps: StepStateResult[] = [{ id: testStepId, properties: [] }];
            const wrapper = ({ children }: { children: React.ReactNode }) => (
                <EducationStepsContext.Provider
                    value={{
                        educationSteps,
                        setEducationSteps: mockSetEducationSteps,
                        educationStepIdsAllowlist: new Set(),
                        setEducationStepIdsAllowlist: mockSetEducationStepIdsAllowlist,
                    }}
                >
                    {children}
                </EducationStepsContext.Provider>
            );

            const { result } = renderHook(() => useHasSeenEducationStep(testStepId, false), { wrapper });

            expect(result.current).toBe(true);
        });
    });

    describe('when user is loaded', () => {
        beforeEach(() => {
            mockUseUserContext.mockReturnValue({ user: { urn: testUserUrn } });
        });

        it('should return true when the step has been seen', () => {
            const educationSteps: StepStateResult[] = [{ id: `${testUserUrn}-${testStepId}`, properties: [] }];
            const wrapper = ({ children }: { children: React.ReactNode }) => (
                <EducationStepsContext.Provider
                    value={{
                        educationSteps,
                        setEducationSteps: mockSetEducationSteps,
                        educationStepIdsAllowlist: new Set(),
                        setEducationStepIdsAllowlist: mockSetEducationStepIdsAllowlist,
                    }}
                >
                    {children}
                </EducationStepsContext.Provider>
            );

            const { result } = renderHook(() => useHasSeenEducationStep(testStepId), { wrapper });

            expect(result.current).toBe(true);
        });

        it('should return false when the step has not been seen', () => {
            const educationSteps: StepStateResult[] = [{ id: `${testUserUrn}-other-step`, properties: [] }];
            const wrapper = ({ children }: { children: React.ReactNode }) => (
                <EducationStepsContext.Provider
                    value={{
                        educationSteps,
                        setEducationSteps: mockSetEducationSteps,
                        educationStepIdsAllowlist: new Set(),
                        setEducationStepIdsAllowlist: mockSetEducationStepIdsAllowlist,
                    }}
                >
                    {children}
                </EducationStepsContext.Provider>
            );

            const { result } = renderHook(() => useHasSeenEducationStep(testStepId), { wrapper });

            expect(result.current).toBe(false);
        });

        it('should return true when educationSteps is null (loading state)', () => {
            const wrapper = ({ children }: { children: React.ReactNode }) => (
                <EducationStepsContext.Provider
                    value={{
                        educationSteps: null,
                        setEducationSteps: mockSetEducationSteps,
                        educationStepIdsAllowlist: new Set(),
                        setEducationStepIdsAllowlist: mockSetEducationStepIdsAllowlist,
                    }}
                >
                    {children}
                </EducationStepsContext.Provider>
            );

            const { result } = renderHook(() => useHasSeenEducationStep(testStepId), { wrapper });

            expect(result.current).toBe(true);
        });

        it('should use non-converted stepId when isForUser=false', () => {
            const educationSteps: StepStateResult[] = [{ id: testStepId, properties: [] }];
            const wrapper = ({ children }: { children: React.ReactNode }) => (
                <EducationStepsContext.Provider
                    value={{
                        educationSteps,
                        setEducationSteps: mockSetEducationSteps,
                        educationStepIdsAllowlist: new Set(),
                        setEducationStepIdsAllowlist: mockSetEducationStepIdsAllowlist,
                    }}
                >
                    {children}
                </EducationStepsContext.Provider>
            );

            const { result } = renderHook(() => useHasSeenEducationStep(testStepId, false), { wrapper });

            expect(result.current).toBe(true);
        });
    });
});

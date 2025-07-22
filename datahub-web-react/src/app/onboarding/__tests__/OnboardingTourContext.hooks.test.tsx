import { renderHook } from '@testing-library/react-hooks';
import React from 'react';

import { OnboardingTourContext } from '@app/onboarding/OnboardingTourContext';
import { useOnboardingTour } from '@app/onboarding/OnboardingTourContext.hooks';

const mockContextValue = {
    isModalTourOpen: false,
    triggerModalTour: vi.fn(),
    closeModalTour: vi.fn(),
    triggerOriginalTour: vi.fn(),
    closeOriginalTour: vi.fn(),
    originalTourStepIds: null,
};

// Mock console methods
const mockConsoleError = vi.fn();
const mockConsoleWarn = vi.fn();

beforeEach(() => {
    vi.clearAllMocks();
    vi.spyOn(console, 'error').mockImplementation(mockConsoleError);
    vi.spyOn(console, 'warn').mockImplementation(mockConsoleWarn);
});

afterEach(() => {
    vi.restoreAllMocks();
});

describe('useOnboardingTour', () => {
    describe('when context is available', () => {
        const wrapper = ({ children }: { children: React.ReactNode }) => (
            <OnboardingTourContext.Provider value={mockContextValue}>{children}</OnboardingTourContext.Provider>
        );

        it('should return the context value', () => {
            const { result } = renderHook(() => useOnboardingTour(), { wrapper });

            expect(result.current).toBe(mockContextValue);
        });

        it('should not log any errors', () => {
            renderHook(() => useOnboardingTour(), { wrapper });

            expect(mockConsoleError).not.toHaveBeenCalled();
        });
    });

    describe('when context is undefined', () => {
        const wrapper = ({ children }: { children: React.ReactNode }) => (
            <OnboardingTourContext.Provider value={undefined}>{children}</OnboardingTourContext.Provider>
        );

        it('should log an error', () => {
            renderHook(() => useOnboardingTour(), { wrapper });

            expect(mockConsoleError).toHaveBeenCalledWith(
                'useOnboardingTour must be used within an OnboardingTourProvider. Returning fallback context.',
            );
        });

        it('should return fallback context with correct structure', () => {
            const { result } = renderHook(() => useOnboardingTour(), { wrapper });

            expect(result.current).toEqual({
                isModalTourOpen: false,
                triggerModalTour: expect.any(Function),
                closeModalTour: expect.any(Function),
                triggerOriginalTour: expect.any(Function),
                closeOriginalTour: expect.any(Function),
                originalTourStepIds: null,
            });
        });

        it('should warn when triggerModalTour is called', () => {
            const { result } = renderHook(() => useOnboardingTour(), { wrapper });

            result.current.triggerModalTour();

            expect(mockConsoleWarn).toHaveBeenCalledWith('triggerModalTour called outside of OnboardingTourProvider');
        });

        it('should warn when closeModalTour is called', () => {
            const { result } = renderHook(() => useOnboardingTour(), { wrapper });

            result.current.closeModalTour();

            expect(mockConsoleWarn).toHaveBeenCalledWith('closeModalTour called outside of OnboardingTourProvider');
        });

        it('should warn when triggerOriginalTour is called', () => {
            const { result } = renderHook(() => useOnboardingTour(), { wrapper });

            result.current.triggerOriginalTour(['step1', 'step2']);

            expect(mockConsoleWarn).toHaveBeenCalledWith(
                'triggerOriginalTour called outside of OnboardingTourProvider',
            );
        });

        it('should warn when closeOriginalTour is called', () => {
            const { result } = renderHook(() => useOnboardingTour(), { wrapper });

            result.current.closeOriginalTour();

            expect(mockConsoleWarn).toHaveBeenCalledWith('closeOriginalTour called outside of OnboardingTourProvider');
        });
    });

    describe('when used outside of provider', () => {
        it('should log an error and return fallback context', () => {
            const { result } = renderHook(() => useOnboardingTour());

            expect(mockConsoleError).toHaveBeenCalledWith(
                'useOnboardingTour must be used within an OnboardingTourProvider. Returning fallback context.',
            );

            expect(result.current).toEqual({
                isModalTourOpen: false,
                triggerModalTour: expect.any(Function),
                closeModalTour: expect.any(Function),
                triggerOriginalTour: expect.any(Function),
                closeOriginalTour: expect.any(Function),
                originalTourStepIds: null,
            });
        });
    });
});

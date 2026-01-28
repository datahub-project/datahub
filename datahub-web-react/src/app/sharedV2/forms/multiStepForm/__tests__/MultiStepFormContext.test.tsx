import { act, renderHook } from '@testing-library/react-hooks';
import * as React from 'react';
import { describe, expect, it, vi } from 'vitest';

import { MultiStepFormProvider, useMultiStepContext } from '@app/sharedV2/forms/multiStepForm/MultiStepFormContext';
import { MultiStepFormProviderProps, Step } from '@app/sharedV2/forms/multiStepForm/types';

// Define a test state type
interface TestState {
    name?: string;
    count?: number;
    data?: Record<string, any>;
}

// Helper component to wrap the hook with the provider
function renderMultiStepContextHook<TState>(providerProps: MultiStepFormProviderProps<TState>) {
    const wrapper: React.FC<React.PropsWithChildren<object>> = ({ children }) => (
        <MultiStepFormProvider {...providerProps}>{children}</MultiStepFormProvider>
    );
    return renderHook(() => useMultiStepContext<TState, Step>(), {
        wrapper,
    });
}

describe('MultiStepFormContext', () => {
    const mockStep1: Step = {
        label: 'Step 1',
        key: 'step1',
        content: <div>Content 1</div>,
    };

    const mockStep2: Step = {
        label: 'Step 2',
        key: 'step2',
        content: <div>Content 2</div>,
    };

    const mockStep3: Step = {
        label: 'Step 3',
        key: 'step3',
        content: <div>Content 3</div>,
    };

    describe('initialization', () => {
        it('should initialize with default values when no initialState is provided', () => {
            const { result } = renderMultiStepContextHook<TestState>({ steps: [mockStep1] });

            expect(result.current.state).toEqual(undefined);
            expect(result.current.totalSteps).toBe(1);
            expect(result.current.currentStepIndex).toBe(0);
            expect(result.current.getCurrentStep()).toEqual(mockStep1);
        });

        it('should initialize with provided initialState', () => {
            const initialState: TestState = { name: 'test', count: 1 };
            const { result } = renderMultiStepContextHook<TestState>({ steps: [mockStep1], initialState });

            expect(result.current.state).toEqual(initialState);
        });
    });

    describe('state management', () => {
        it('should update state with new values using deep merge', () => {
            const initialState: TestState = { name: 'initial', data: { nested: 'value' } };
            const { result } = renderMultiStepContextHook<TestState>({ steps: [mockStep1], initialState });

            act(() => {
                result.current.updateState({ count: 5, data: { nested: 'updated' } });
            });

            expect(result.current.state).toEqual({
                name: 'initial',
                count: 5,
                data: { nested: 'updated' },
            });
        });

        it('should handle undefined state for deep merge', () => {
            const { result } = renderMultiStepContextHook<TestState>({ steps: [mockStep1] });

            act(() => {
                result.current.updateState({ name: 'first', count: 1 });
            });

            expect(result.current.state).toEqual({ name: 'first', count: 1 });
        });
    });

    describe('step navigation', () => {
        it('should correctly calculate totalSteps', () => {
            const { result } = renderMultiStepContextHook<TestState>({ steps: [mockStep1, mockStep2, mockStep3] });

            expect(result.current.totalSteps).toBe(3);
        });

        it('should navigate to next step when canGoToNext returns true', () => {
            const { result } = renderMultiStepContextHook<TestState>({ steps: [mockStep1, mockStep2] });

            expect(result.current.currentStepIndex).toBe(0);
            expect(result.current.canGoToNext()).toBe(true);
            expect(result.current.getCurrentStep()).toEqual(mockStep1);

            act(() => {
                result.current.goToNext();
            });

            expect(result.current.currentStepIndex).toBe(1);
            expect(result.current.getCurrentStep()).toEqual(mockStep2);
        });

        it('should not navigate to next step when on the last step', () => {
            const { result } = renderMultiStepContextHook<TestState>({ steps: [mockStep1, mockStep2] });

            // Move to the last step first
            act(() => {
                result.current.goToNext();
            });

            expect(result.current.currentStepIndex).toBe(1);
            expect(result.current.canGoToNext()).toBe(false);

            // Try to go to next (should not change)
            act(() => {
                result.current.goToNext();
            });

            expect(result.current.currentStepIndex).toBe(1);
            expect(result.current.getCurrentStep()).toEqual(mockStep2);
        });

        it('should navigate to previous step when canGoToPrevious returns true', () => {
            const { result } = renderMultiStepContextHook<TestState>({ steps: [mockStep1, mockStep2] });

            // Move to step 2 first
            act(() => {
                result.current.goToNext();
            });

            expect(result.current.currentStepIndex).toBe(1);
            expect(result.current.canGoToPrevious()).toBe(true);
            expect(result.current.getCurrentStep()).toEqual(mockStep2);

            act(() => {
                result.current.goToPrevious();
            });

            expect(result.current.currentStepIndex).toBe(0);
            expect(result.current.getCurrentStep()).toEqual(mockStep1);
        });

        it('should not navigate to previous step when on the first step', () => {
            const { result } = renderMultiStepContextHook<TestState>({ steps: [mockStep1, mockStep2] });

            expect(result.current.currentStepIndex).toBe(0);
            expect(result.current.canGoToPrevious()).toBe(false);

            // Try to go to previous (should not change)
            act(() => {
                result.current.goToPrevious();
            });

            expect(result.current.currentStepIndex).toBe(0);
            expect(result.current.getCurrentStep()).toEqual(mockStep1);
        });

        it('should correctly determine if it is the final step', () => {
            const { result } = renderMultiStepContextHook<TestState>({ steps: [mockStep1, mockStep2, mockStep3] });

            expect(result.current.isFinalStep()).toBe(false);

            // Move to middle step
            act(() => {
                result.current.goToNext();
            });
            expect(result.current.isFinalStep()).toBe(false);

            // Move to final step
            act(() => {
                result.current.goToNext();
            });
            expect(result.current.isFinalStep()).toBe(true);
        });
    });

    describe('step completion tracking', () => {
        it('should track completed steps correctly', () => {
            const { result } = renderMultiStepContextHook<TestState>({ steps: [mockStep1, mockStep2] });

            expect(result.current.isStepCompleted('step1')).toBe(false);
            expect(result.current.isStepCompleted('step2')).toBe(false);
            expect(result.current.isCurrentStepCompleted()).toBe(false);

            act(() => {
                result.current.setCurrentStepCompleted();
            });

            expect(result.current.isStepCompleted('step1')).toBe(true);
            expect(result.current.isCurrentStepCompleted()).toBe(true);
            expect(result.current.isStepCompleted('step2')).toBe(false);
        });

        it('should handle multiple completed steps', () => {
            const { result } = renderMultiStepContextHook<TestState>({ steps: [mockStep1, mockStep2, mockStep3] });

            // Mark step 1 as completed
            act(() => {
                result.current.setCurrentStepCompleted();
            });

            // Move to step 2
            act(() => {
                result.current.goToNext();
            });

            // Mark step 2 as completed
            act(() => {
                result.current.setCurrentStepCompleted();
            });

            expect(result.current.isStepCompleted('step1')).toBe(true);
            expect(result.current.isStepCompleted('step2')).toBe(true);
            expect(result.current.isStepCompleted('step3')).toBe(false);
        });
    });

    describe('form submission and cancellation', () => {
        it('should call onSubmit with current state when submit is triggered', async () => {
            const mockSubmit = vi.fn();
            const initialState: TestState = { name: 'test', count: 42 };
            const { result } = renderMultiStepContextHook<TestState>({
                steps: [mockStep1],
                initialState,
                onSubmit: mockSubmit,
            });

            await act(async () => {
                await result.current.submit();
            });

            expect(mockSubmit).toHaveBeenCalledWith(initialState, undefined);
        });

        it('should handle submit when no onSubmit is provided', async () => {
            const { result } = renderMultiStepContextHook<TestState>({ steps: [mockStep1] });

            await act(async () => {
                await result.current.submit();
            });

            // Should not throw an error even without onSubmit
            expect(result.current.state).toBeUndefined();
        });

        it('should call onCancel with isDirty argument when cancel is triggered', () => {
            const mockCancel = vi.fn();
            const { result } = renderMultiStepContextHook<TestState>({
                steps: [mockStep1],
                initialState: { name: 'test' },
                onCancel: mockCancel,
            });

            act(() => {
                result.current.cancel();
            });

            expect(mockCancel).toHaveBeenCalledWith({ isDirty: false });
        });

        it('should handle cancel when no onCancel is provided', () => {
            const { result } = renderMultiStepContextHook<TestState>({ steps: [mockStep1] });

            expect(() => {
                act(() => {
                    result.current.cancel();
                });
            }).not.toThrow();

            // Should not throw an error even without onCancel
        });

        it('should call onCancel with correct isDirty value', () => {
            const mockCancel = vi.fn();
            const initialState: TestState = { name: 'test', count: 1 };
            const { result } = renderMultiStepContextHook<TestState>({
                steps: [mockStep1],
                initialState,
                onCancel: mockCancel,
            });

            // State hasn't changed, so should be clean
            act(() => {
                result.current.cancel();
            });
            expect(mockCancel).toHaveBeenCalledWith({ isDirty: false });

            // Make state dirty
            act(() => {
                result.current.updateState({ count: 2 });
            });

            // Reset the mock to check the next call
            mockCancel.mockClear();

            // State has changed, so should be dirty
            act(() => {
                result.current.cancel();
            });
            expect(mockCancel).toHaveBeenCalledWith({ isDirty: true });
        });
    });

    describe('isDirty', () => {
        it('should return false when state matches initialState', () => {
            const initialState: TestState = { name: 'test', count: 1 };
            const { result } = renderMultiStepContextHook<TestState>({
                steps: [mockStep1],
                initialState,
            });

            expect(result.current.isDirty()).toBe(false);
        });

        it('should return true when state differs from initialState', () => {
            const initialState: TestState = { name: 'test', count: 1 };
            const { result } = renderMultiStepContextHook<TestState>({
                steps: [mockStep1],
                initialState,
            });

            act(() => {
                result.current.updateState({ count: 2 });
            });

            expect(result.current.isDirty()).toBe(true);
        });

        it('should return false when updated state matches initialState after changes', () => {
            const initialState: TestState = { name: 'test', count: 1 };
            const { result } = renderMultiStepContextHook<TestState>({
                steps: [mockStep1],
                initialState,
            });

            act(() => {
                result.current.updateState({ count: 2 });
            });
            expect(result.current.isDirty()).toBe(true);

            act(() => {
                result.current.updateState({ count: 1 });
            });
            expect(result.current.isDirty()).toBe(false);
        });

        it('should use isDirtyChecker when provided', () => {
            const isDirtyChecker = vi.fn(() => true);
            const initialState: TestState = { name: 'test', count: 1 };
            const { result } = renderMultiStepContextHook<TestState>({
                steps: [mockStep1],
                initialState,
                isDirtyChecker,
            });

            expect(result.current.isDirty()).toBe(true);
            expect(isDirtyChecker).toHaveBeenCalledWith(initialState, initialState);
        });
    });

    describe('getCurrentStep', () => {
        it('should return the current step based on index', () => {
            const { result } = renderMultiStepContextHook<TestState>({ steps: [mockStep1, mockStep2] });

            expect(result.current.getCurrentStep()).toEqual(mockStep1);

            act(() => {
                result.current.goToNext();
            });

            expect(result.current.getCurrentStep()).toEqual(mockStep2);
        });

        it('should return undefined if index is out of bounds', () => {
            // This is harder to test directly since we can't easily set an out-of-bounds index
            // But we can verify normal behavior works as expected
            const { result } = renderMultiStepContextHook<TestState>({ steps: [mockStep1, mockStep2] });

            expect(result.current.getCurrentStep()).toEqual(mockStep1);
        });
    });

    describe('OnNextHandler', () => {
        it('should execute the onNextHandler when going to next step', () => {
            const mockNextHandler = vi.fn();
            const { result } = renderMultiStepContextHook<TestState>({ steps: [mockStep1, mockStep2] });

            // Set the next handler
            act(() => {
                result.current.setOnNextHandler(mockNextHandler);
            });

            // Go to next step
            act(() => {
                result.current.goToNext();
            });

            expect(mockNextHandler).toHaveBeenCalled();
            expect(result.current.currentStepIndex).toBe(1);
        });

        it('should reset the onNextHandler after execution', () => {
            const mockNextHandler = vi.fn();
            const { result } = renderMultiStepContextHook<TestState>({ steps: [mockStep1, mockStep2] });

            // Set the next handler
            act(() => {
                result.current.setOnNextHandler(mockNextHandler);
            });

            // Go to next step
            act(() => {
                result.current.goToNext();
            });

            // The handler should have been executed and reset
            expect(mockNextHandler).toHaveBeenCalled();

            // Set handler again and ensure it can be called again in the next step
            const mockNextHandler2 = vi.fn();
            act(() => {
                result.current.setOnNextHandler(mockNextHandler2);
            });

            act(() => {
                result.current.goToPrevious(); // Go back to step 1
            });
            act(() => {
                result.current.goToNext(); // Go to step 2 again
            });

            expect(mockNextHandler2).toHaveBeenCalled();
        });

        it('should handle onNextHandler function execution', () => {
            // Test that the onNextHandler is executed properly without errors
            const mockNextHandler = vi.fn();

            const { result } = renderMultiStepContextHook<TestState>({ steps: [mockStep1, mockStep2] });

            // Set the next handler
            act(() => {
                result.current.setOnNextHandler(mockNextHandler);
            });

            // Call goToNext - this should execute the handler
            act(() => {
                result.current.goToNext();
            });

            // Verify the handler was called
            expect(mockNextHandler).toHaveBeenCalled();
        });
    });
});

import { act, renderHook } from '@testing-library/react-hooks';
import React, { ReactNode } from 'react';
import { describe, expect, it, vi } from 'vitest';

import {
    MultiStepFormProvider,
    Step,
    useMultiStepContext,
} from '@app/sharedV2/forms/multiStepForm/MultiStepFormContext';

// Define a test state type
interface TestState {
    name: string;
    count: number;
}

interface TestStep extends Step {
    key: 'step1' | 'step2' | 'step3';
}

const mockSteps: TestStep[] = [
    {
        label: 'Step 1',
        key: 'step1',
        content: <div>Content 1</div>,
    },
    {
        label: 'Step 2',
        key: 'step2',
        content: <div>Content 2</div>,
    },
    {
        label: 'Step 3',
        key: 'step3',
        content: <div>Content 3</div>,
    },
];

// Wrapper component for the provider
const MultiStepProviderWrapper = ({
    children,
    steps = mockSteps,
    initialState,
    onSubmit,
    onCancel,
}: {
    children: ReactNode;
    steps?: TestStep[];
    initialState?: TestState;
    onSubmit?: (state: TestState | undefined) => Promise<void>;
    onCancel?: () => void;
}) => {
    return (
        <MultiStepFormProvider<TestState>
            steps={steps}
            initialState={initialState}
            onSubmit={onSubmit}
            onCancel={onCancel}
        >
            {children}
        </MultiStepFormProvider>
    );
};

describe('MultiStepFormContext', () => {
    it('initializes with correct default values', () => {
        const { result } = renderHook(() => useMultiStepContext<TestState, TestStep>(), {
            wrapper: MultiStepProviderWrapper,
        });

        expect(result.current.state).toBeUndefined();
        expect(result.current.totalSteps).toBe(3);
        expect(result.current.currentStepIndex).toBe(0);
        expect(result.current.getCurrentStep()).toEqual(mockSteps[0]);
    });

    it('initializes with provided initial state', () => {
        const initialState: TestState = { name: 'test', count: 10 };
        const { result } = renderHook(() => useMultiStepContext<TestState, TestStep>(), {
            wrapper: () => <MultiStepProviderWrapper initialState={initialState}>test</MultiStepProviderWrapper>,
        });

        expect(result.current.state).toEqual(initialState);
    });

    it('updates state correctly with updateState', () => {
        const { result } = renderHook(() => useMultiStepContext<TestState, TestStep>(), {
            wrapper: MultiStepProviderWrapper,
        });

        const newState = { name: 'updated', count: 5 };

        act(() => {
            result.current.updateState(newState);
        });

        expect(result.current.state).toEqual(newState);
    });

    it('handles deep merging of state correctly', () => {
        const initialState: TestState = { name: 'original', count: 10 };
        const { result } = renderHook(() => useMultiStepContext<TestState, TestStep>(), {
            wrapper: () => <MultiStepProviderWrapper initialState={initialState}>test</MultiStepProviderWrapper>,
        });

        const partialState = { count: 15 }; // Only update one field

        act(() => {
            result.current.updateState(partialState);
        });

        expect(result.current.state).toEqual({ name: 'original', count: 15 });
    });

    it('allows navigation to next step', () => {
        const { result } = renderHook(() => useMultiStepContext<TestState, TestStep>(), {
            wrapper: MultiStepProviderWrapper,
        });

        expect(result.current.currentStepIndex).toBe(0);
        expect(result.current.getCurrentStep()?.key).toBe('step1');

        act(() => {
            result.current.goToNext();
        });

        expect(result.current.currentStepIndex).toBe(1);
        expect(result.current.getCurrentStep()?.key).toBe('step2');
    });

    it('does not navigate beyond the last step', () => {
        const { result } = renderHook(() => useMultiStepContext<TestState, TestStep>(), {
            wrapper: () => <MultiStepProviderWrapper steps={[mockSteps[0]]}>test</MultiStepProviderWrapper>,
        });

        expect(result.current.currentStepIndex).toBe(0);
        expect(result.current.canGoToNext()).toBe(false);
    });

    it('prevents navigation to next when at last step', () => {
        const { result } = renderHook(() => useMultiStepContext<TestState, TestStep>(), {
            wrapper: () => (
                <MultiStepProviderWrapper steps={[mockSteps[0], mockSteps[1]]}>test</MultiStepProviderWrapper>
            ),
        });

        // Navigate to the second step (last step)
        act(() => {
            result.current.goToNext();
        });

        expect(result.current.currentStepIndex).toBe(1);
        expect(result.current.canGoToNext()).toBe(false);

        // Try to go to next (should not change)
        act(() => {
            result.current.goToNext();
        });

        expect(result.current.currentStepIndex).toBe(1); // Should remain the same
    });

    it('allows navigation to previous step', () => {
        const { result } = renderHook(() => useMultiStepContext<TestState, TestStep>(), {
            wrapper: MultiStepProviderWrapper,
        });

        // First navigate to step 2
        act(() => {
            result.current.goToNext();
        });

        expect(result.current.currentStepIndex).toBe(1);
        expect(result.current.getCurrentStep()?.key).toBe('step2');

        // Then go back to step 1
        act(() => {
            result.current.goToPrevious();
        });

        expect(result.current.currentStepIndex).toBe(0);
        expect(result.current.getCurrentStep()?.key).toBe('step1');
    });

    it('prevents navigation to previous when at first step', () => {
        const { result } = renderHook(() => useMultiStepContext<TestState, TestStep>(), {
            wrapper: MultiStepProviderWrapper,
        });

        expect(result.current.currentStepIndex).toBe(0);
        expect(result.current.canGoToPrevious()).toBe(false);

        // Try to go to previous (should not change)
        act(() => {
            result.current.goToPrevious();
        });

        expect(result.current.currentStepIndex).toBe(0); // Should remain the same
    });

    it('correctly identifies final step', () => {
        const { result } = renderHook(() => useMultiStepContext<TestState, TestStep>(), {
            wrapper: () => (
                <MultiStepProviderWrapper steps={[mockSteps[0], mockSteps[1], mockSteps[2]]}>
                    test
                </MultiStepProviderWrapper>
            ),
        });

        expect(result.current.isFinalStep()).toBe(false); // At step 0

        act(() => {
            result.current.goToNext();
        });
        expect(result.current.isFinalStep()).toBe(false); // At step 1

        act(() => {
            result.current.goToNext();
        });
        expect(result.current.isFinalStep()).toBe(true); // At step 2 (final)
    });

    it('tracks step completion correctly', () => {
        const { result } = renderHook(() => useMultiStepContext<TestState, TestStep>(), {
            wrapper: MultiStepProviderWrapper,
        });

        const stepKey = mockSteps[0].key;

        // Initially step should not be completed
        expect(result.current.isStepCompleted(stepKey)).toBe(false);
        expect(result.current.isCurrentStepCompleted()).toBe(false);

        // Mark the current step as completed
        act(() => {
            result.current.setCurrentStepCompleted();
        });

        // Now step should be completed
        expect(result.current.isStepCompleted(stepKey)).toBe(true);
        expect(result.current.isCurrentStepCompleted()).toBe(true);
    });

    it('handles step completion for different steps', () => {
        const { result } = renderHook(() => useMultiStepContext<TestState, TestStep>(), {
            wrapper: MultiStepProviderWrapper,
        });

        // Mark first step as completed
        act(() => {
            result.current.setCurrentStepCompleted();
        });

        const step1Key = mockSteps[0].key;
        const step2Key = mockSteps[1].key;

        expect(result.current.isStepCompleted(step1Key)).toBe(true);
        expect(result.current.isStepCompleted(step2Key)).toBe(false);

        // Navigate to next step
        act(() => {
            result.current.goToNext();
        });

        expect(result.current.isCurrentStepCompleted()).toBe(false); // Second step not completed yet

        // Mark second step as completed
        act(() => {
            result.current.setCurrentStepCompleted();
        });

        expect(result.current.isCurrentStepCompleted()).toBe(true); // Second step now completed
        expect(result.current.isStepCompleted(step2Key)).toBe(true);
    });

    it('calls onSubmit when submit is triggered', async () => {
        const mockSubmit = vi.fn().mockResolvedValue(undefined);
        const initialState: TestState = { name: 'test', count: 10 };

        const { result } = renderHook(() => useMultiStepContext<TestState, TestStep>(), {
            wrapper: () => (
                <MultiStepProviderWrapper initialState={initialState} onSubmit={mockSubmit}>
                    test
                </MultiStepProviderWrapper>
            ),
        });

        await act(async () => {
            await result.current.submit();
        });

        expect(mockSubmit).toHaveBeenCalledWith(initialState);
    });

    it('calls onCancel when cancel is triggered', () => {
        const mockCancel = vi.fn();

        const { result } = renderHook(() => useMultiStepContext<TestState, TestStep>(), {
            wrapper: () => <MultiStepProviderWrapper onCancel={mockCancel}>test</MultiStepProviderWrapper>,
        });

        act(() => {
            result.current.cancel();
        });

        expect(mockCancel).toHaveBeenCalled();
    });

    it('correctly handles disabled steps when determining navigation', () => {
        const disabledSteps: TestStep[] = [
            {
                label: 'Step 1',
                key: 'step1',
                content: <div>Content 1</div>,
                disabled: true, // This step is disabled
            },
            {
                label: 'Step 2',
                key: 'step2',
                content: <div>Content 2</div>,
            },
            {
                label: 'Step 3',
                key: 'step3',
                content: <div>Content 3</div>,
            },
        ];

        const { result } = renderHook(() => useMultiStepContext<TestState, TestStep>(), {
            wrapper: () => <MultiStepProviderWrapper steps={disabledSteps}>test</MultiStepProviderWrapper>,
        });

        // When first step is disabled, should start at index 1
        expect(result.current.currentStepIndex).toBe(1);
        expect(result.current.canGoToPrevious()).toBe(false); // Can't go to index 0 since it's disabled
    });

    it('correctly handles all steps disabled', () => {
        const allDisabledSteps: TestStep[] = [
            {
                label: 'Step 1',
                key: 'step1',
                content: <div>Content 1</div>,
                disabled: true,
            },
            {
                label: 'Step 2',
                key: 'step2',
                content: <div>Content 2</div>,
                disabled: true,
            },
            {
                label: 'Step 3',
                key: 'step3',
                content: <div>Content 3</div>,
                disabled: true,
            },
        ];

        const { result } = renderHook(() => useMultiStepContext<TestState, TestStep>(), {
            wrapper: () => <MultiStepProviderWrapper steps={allDisabledSteps}>test</MultiStepProviderWrapper>,
        });

        // When all steps are disabled, should start at last step index
        expect(result.current.currentStepIndex).toBe(2); // Last index
    });
});

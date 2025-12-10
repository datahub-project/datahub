import React, { useCallback, useMemo, useState } from 'react';
import { deepMerge } from 'remirror';

import { Step, StepKey } from '@app/sharedV2/forms/multiStepForm/types';

export interface MultiStepFormContextType<TState, TStep extends Step = Step> {
    state: TState;
    updateState: (newState: Partial<TState>) => void;

    submit: () => Promise<void>;
    cancel: () => void;

    totalSteps: number;
    currentStepIndex: number;
    getCurrentStep: () => TStep | undefined;

    canGoToNext: () => boolean;
    goToNext: () => void;
    canGoToPrevious: () => boolean;
    goToPrevious: () => void;
    isFinalStep: () => boolean;

    isStepCompleted: (stepKey: StepKey) => boolean;
    isCurrentStepCompleted: () => boolean;
    setCurrentStepCompleted: () => void;
}

const MultiStepContext = React.createContext<MultiStepFormContextType<any, any>>({
    state: {},
    updateState: () => null,
    canGoToNext: () => false,
    goToNext: () => null,
    canGoToPrevious: () => false,
    goToPrevious: () => null,
    isFinalStep: () => false,
    isStepCompleted: () => false,
    isCurrentStepCompleted: () => false,
    setCurrentStepCompleted: () => null,

    getCurrentStep: () => undefined,
    submit: () =>
        new Promise<void>((resolve) => {
            resolve();
        }),
    cancel: () => null,

    totalSteps: 0,
    currentStepIndex: 0,
});

export function useMultiStepContext<TState, TStep extends Step>() {
    return React.useContext<MultiStepFormContextType<TState, TStep>>(MultiStepContext);
}

export interface MultiStepFormProviderProps<TState> {
    steps: Step[];
    initialState?: TState;
    onSubmit?: (state: TState | undefined) => Promise<void>;
    onCancel?: () => void;
}

export function MultiStepFormProvider<TState>({
    children,
    steps,
    initialState,
    onSubmit,
    onCancel,
}: React.PropsWithChildren<MultiStepFormProviderProps<TState>>) {
    const [state, setState] = useState<TState | undefined>(initialState);

    const [completedSteps, setCompletedSteps] = useState<Set<StepKey>>(new Set());

    const totalSteps = useMemo(() => steps.length, [steps]);

    const updateState = useCallback((newState: Partial<TState>) => {
        setState((currentState) => deepMerge(currentState ?? {}, newState));
    }, []);

    const [currentStepIndex, setCurrentStepIndex] = useState<number>(0);

    const getCurrentStep = useCallback(() => {
        return steps?.[currentStepIndex];
    }, [currentStepIndex, steps]);

    const canGoToNext = useCallback(() => {
        return currentStepIndex < totalSteps - 1;
    }, [currentStepIndex, totalSteps]);

    const goToNext = useCallback(() => {
        if (canGoToNext()) {
            setCurrentStepIndex((currentIndex) => currentIndex + 1);
        }
    }, [canGoToNext]);

    const canGoToPrevious = useCallback(() => {
        return currentStepIndex > 0;
    }, [currentStepIndex]);

    const goToPrevious = useCallback(() => {
        if (canGoToPrevious()) {
            setCurrentStepIndex((currentIndex) => currentIndex - 1);
        }
    }, [canGoToPrevious]);

    const isFinalStep = useCallback(() => {
        return currentStepIndex === totalSteps - 1;
    }, [currentStepIndex, totalSteps]);

    const isStepCompleted = useCallback((stepKey: StepKey) => completedSteps.has(stepKey), [completedSteps]);
    const isCurrentStepCompleted = useCallback(
        () => isStepCompleted(getCurrentStep()?.key),
        [isStepCompleted, getCurrentStep],
    );

    const setCurrentStepCompleted = useCallback(() => {
        setCompletedSteps((currentCompletedSteps) => new Set([...currentCompletedSteps, getCurrentStep()?.key]));
    }, [getCurrentStep]);

    const submit = useCallback(async () => {
        await onSubmit?.(state);
    }, [onSubmit, state]);

    const cancel = useCallback(() => {
        onCancel?.();
    }, [onCancel]);

    return (
        <MultiStepContext.Provider
            value={{
                state,
                updateState,
                submit,
                cancel,

                totalSteps,
                currentStepIndex,
                getCurrentStep,

                canGoToNext,
                goToNext,
                canGoToPrevious,
                goToPrevious,
                isFinalStep,
                isStepCompleted,
                isCurrentStepCompleted,
                setCurrentStepCompleted,
            }}
        >
            {children}
        </MultiStepContext.Provider>
    );
}

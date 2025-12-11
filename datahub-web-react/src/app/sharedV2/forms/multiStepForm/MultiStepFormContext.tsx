import React, { useCallback, useMemo, useState } from 'react';
import { deepMerge } from 'remirror';

import { MultiStepFormContextType, OnNextHandler, Step, StepKey } from '@app/sharedV2/forms/multiStepForm/types';

const MultiStepContext = React.createContext<MultiStepFormContextType<any, any>>({
    state: {},
    updateState: () => null,
    canGoToNext: () => false,
    goToNext: () => null,
    canGoToPrevious: () => false,
    setOnNextHandler: () => {},
    goToPrevious: () => null,
    goToStep: () => null,
    isFinalStep: () => false,
    isStepVisited: () => false,
    isStepCompleted: () => false,
    isCurrentStepCompleted: () => false,
    setCurrentStepCompleted: () => null,
    setCurrentStepUncompleted: () => null,

    getCurrentStep: () => undefined,
    submit: () =>
        new Promise<void>((resolve) => {
            resolve();
        }),
    cancel: () => null,

    steps: [],
    totalSteps: 0,
    currentStepIndex: 0,
});

export function useMultiStepContext<TState, TStep extends Step, TSubmitOptions = any>() {
    return React.useContext<MultiStepFormContextType<TState, TStep, TSubmitOptions>>(MultiStepContext);
}

export interface MultiStepFormProviderProps<TState, TSubmitOptions = any> {
    steps: Step[];
    initialState?: TState;
    onSubmit?: (state: TState | undefined, options?: TSubmitOptions) => Promise<void>;
    onCancel?: () => void;
}

export function MultiStepFormProvider<TState, TSubmitOptions = any>({
    children,
    steps,
    initialState,
    onSubmit,
    onCancel,
}: React.PropsWithChildren<MultiStepFormProviderProps<TState>>) {
    const [state, setState] = useState<TState | undefined>(initialState);
    const [onNextHandler, setOnNextHandler] = useState<OnNextHandler | undefined>();

    const [completedSteps, setCompletedSteps] = useState<Set<StepKey>>(new Set());
    const [visitedSteps, setVisitedSteps] = useState<Set<StepKey>>(new Set());

    const totalSteps = useMemo(() => steps.length, [steps]);

    const updateState = useCallback((newState: Partial<TState>) => {
        setState((currentState) => deepMerge(currentState ?? {}, newState));
    }, []);

    const [currentStepIndex, setCurrentStepIndex] = useState<number>(0);
    const getCurrentStep = useCallback(() => {
        const currentStep = steps?.[currentStepIndex];

        if (currentStep && !visitedSteps.has(currentStep.key)) {
            setVisitedSteps((currentVisitedSteps) => new Set([...currentVisitedSteps, currentStep?.key]));
        }

        return currentStep;
    }, [currentStepIndex, steps, visitedSteps]);

    const canGoToNext = useCallback(() => {
        return currentStepIndex < totalSteps - 1;
    }, [currentStepIndex, totalSteps]);

    const goToNext = useCallback(() => {
        if (canGoToNext()) {
            try {
                onNextHandler?.();
                setCurrentStepIndex((currentIndex) => currentIndex + 1);
                setOnNextHandler(undefined);
            } catch (e) {
                console.error('Go to next error', e);
            }
        }
    }, [canGoToNext, onNextHandler]);

    const canGoToPrevious = useCallback(() => {
        return currentStepIndex > 0;
    }, [currentStepIndex]);

    const goToPrevious = useCallback(() => {
        if (canGoToPrevious()) {
            setCurrentStepIndex((currentIndex) => currentIndex - 1);
        }
    }, [canGoToPrevious]);

    const goToStep = useCallback(
        (key: StepKey) => {
            const stepIndex = steps.findIndex((step) => step.key === key);
            if (stepIndex !== -1) {
                setCurrentStepIndex(stepIndex);
            }
        },
        [steps],
    );

    const isFinalStep = useCallback(() => {
        return currentStepIndex === totalSteps - 1;
    }, [currentStepIndex, totalSteps]);

    const isStepVisited = useCallback((stepKey: StepKey) => visitedSteps.has(stepKey), [visitedSteps]);

    const isStepCompleted = useCallback((stepKey: StepKey) => completedSteps.has(stepKey), [completedSteps]);
    const isCurrentStepCompleted = useCallback(
        () => isStepCompleted(getCurrentStep()?.key),
        [isStepCompleted, getCurrentStep],
    );

    const setCurrentStepCompleted = useCallback(() => {
        setCompletedSteps((currentCompletedSteps) => new Set([...currentCompletedSteps, getCurrentStep()?.key]));
    }, [getCurrentStep]);

    const setCurrentStepUncompleted = useCallback(() => {
        setCompletedSteps(
            (currentCompletedSteps) =>
                new Set(Array.from(currentCompletedSteps).filter((stepKey) => stepKey !== getCurrentStep()?.key)),
        );
    }, [getCurrentStep]);

    const submit = useCallback(
        async (options?: TSubmitOptions) => {
            await onSubmit?.(state, options);
        },
        [onSubmit, state],
    );

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

                steps,
                totalSteps,
                currentStepIndex,
                getCurrentStep,

                canGoToNext,
                setOnNextHandler,
                goToNext,
                canGoToPrevious,
                goToPrevious,
                goToStep,
                isFinalStep,
                isStepVisited,
                isStepCompleted,
                isCurrentStepCompleted,
                setCurrentStepCompleted,
                setCurrentStepUncompleted,
            }}
        >
            {children}
        </MultiStepContext.Provider>
    );
}

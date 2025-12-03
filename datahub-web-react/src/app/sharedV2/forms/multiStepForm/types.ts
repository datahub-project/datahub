import React from "react";

export type StepKey = string;

export interface Step {
    label: string;
    subTitle?: React.ReactNode;
    key: StepKey;
    content: React.ReactNode;
    completedByDefault?: boolean;
}

export type OnNextHandler = () => void;

export interface MultiStepFormContextType<TState, TStep extends Step = Step> {
    state: TState;
    updateState: (newState: Partial<TState>) => void;

    submit: () => void;
    cancel: () => void;

    totalSteps: number;
    currentStepIndex: number;
    getCurrentStep: () => TStep | undefined;

    canGoToNext: () => boolean;
    setOnNextHandler: (handler: OnNextHandler | undefined) => void;
    goToNext: () => void;
    canGoToPrevious: () => boolean;
    goToPrevious: () => void;
    isFinalStep: () => boolean;

    isStepCompleted: (stepKey: StepKey) => boolean;
    isCurrentStepCompleted: () => boolean;
    setCurrentStepCompleted: () => void;
    setCurrentStepUncompleted: () => void;
}

export interface MultiStepFormProviderProps<TState> {
    steps: Step[];
    initialState?: TState;
    onSubmit?: (state: TState | undefined) => Promise<void>;
    onCancel?: () => void;
}

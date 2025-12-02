export type StepKey = string;

export interface Step {
    label: string;
    key: StepKey;
    content: React.ReactNode;
}

export interface MultiStepFormContextType<TState, TStep extends Step = Step> {
    state: TState;
    updateState: (newState: Partial<TState>) => void;

    submit: () => void;
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

export interface MultiStepFormProviderProps<TState> {
    steps: Step[];
    initialState?: TState;
    onSubmit?: (state: TState | undefined) => Promise<void>;
    onCancel?: () => void;
}

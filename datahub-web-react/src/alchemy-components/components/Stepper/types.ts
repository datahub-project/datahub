export type StepperStep = {
    title: string;
};

export type StepperProps = {
    steps: StepperStep[];
    currentStepIndex: number;
    dataTestId?: string;
};

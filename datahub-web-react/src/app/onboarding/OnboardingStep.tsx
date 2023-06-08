import { ReactNode } from 'react';

export type OnboardingStep = {
    id?: string;
    title?: string | ReactNode;
    content?: ReactNode;
    selector?: string;
    style?: any;
};

// Conditional steps are steps we should show if the user has already seen its pre-requisite step
// If they have not, then they should never see the conditional step since they will see the
// pre-requisite step next time they encounter it on the screen.
export type ConditionalStep = {
    stepId: string;
    preRequisiteStepId: string;
};

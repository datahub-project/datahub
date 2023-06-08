import { ReactNode } from 'react';

// Conditional steps are steps with a preRequisiteStepId. We should only it show if the user has
// already seen its pre-requisite step. If they have not, then they should never see the conditional
// step since they will see the pre-requisite step next time they encounter it on the screen.
export type OnboardingStep = {
    id?: string;
    preRequisiteStepId?: string;
    title?: string | ReactNode;
    content?: ReactNode;
    selector?: string;
    style?: any;
};

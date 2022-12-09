import { ReactNode } from 'react';

export type OnboardingStep = {
    id?: string;
    title?: string | ReactNode;
    content?: ReactNode;
    selector?: string;
    style?: any;
};

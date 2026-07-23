import { createContext } from 'react';

export type OnboardingTourContextType = {
    isModalTourOpen: boolean;
    triggerModalTour: () => void;
    closeModalTour: () => void;
    triggerOriginalTour: (stepIds: string[]) => void;
    closeOriginalTour: () => void;
    originalTourStepIds: string[] | null;
};

export const OnboardingTourContext = createContext<OnboardingTourContextType | undefined>(undefined);

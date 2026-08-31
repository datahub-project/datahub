import React, { ReactNode, useState } from 'react';

import { OnboardingTourContext } from '@app/onboarding/OnboardingTourContext';

type Props = {
    children: ReactNode;
};

export default function OnboardingTourProvider({ children }: Props) {
    const [isModalTourOpen, setIsModalTourOpen] = useState(false);
    const [originalTourStepIds, setOriginalTourStepIds] = useState<string[] | null>(null);

    const triggerModalTour = () => {
        setIsModalTourOpen(true);
    };

    const closeModalTour = () => {
        setIsModalTourOpen(false);
    };

    const triggerOriginalTour = (stepIds: string[]) => {
        setOriginalTourStepIds(stepIds);
    };

    const closeOriginalTour = () => {
        setOriginalTourStepIds(null);
    };

    const value = {
        isModalTourOpen,
        triggerModalTour,
        closeModalTour,
        triggerOriginalTour,
        closeOriginalTour,
        originalTourStepIds,
    };

    return <OnboardingTourContext.Provider value={value}>{children}</OnboardingTourContext.Provider>;
}

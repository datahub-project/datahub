/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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

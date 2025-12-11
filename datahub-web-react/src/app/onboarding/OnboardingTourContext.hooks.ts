/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useContext } from 'react';

import { OnboardingTourContext, OnboardingTourContextType } from '@app/onboarding/OnboardingTourContext';

export const useOnboardingTour = (): OnboardingTourContextType => {
    const context = useContext(OnboardingTourContext);
    if (context === undefined) {
        console.error('useOnboardingTour must be used within an OnboardingTourProvider. Returning fallback context.');

        // Return a fallback context to prevent app crash
        return {
            isModalTourOpen: false,
            triggerModalTour: () => console.warn('triggerModalTour called outside of OnboardingTourProvider'),
            closeModalTour: () => console.warn('closeModalTour called outside of OnboardingTourProvider'),
            triggerOriginalTour: () => console.warn('triggerOriginalTour called outside of OnboardingTourProvider'),
            closeOriginalTour: () => console.warn('closeOriginalTour called outside of OnboardingTourProvider'),
            originalTourStepIds: null,
        };
    }
    return context;
};

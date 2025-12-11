/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useContext, useEffect } from 'react';

import OnboardingContext from '@app/onboarding/OnboardingContext';

export const useHandleOnboardingTour = () => {
    const { setTourReshow, setIsTourOpen } = useContext(OnboardingContext);

    const showOnboardingTour = () => {
        setTourReshow(true);
        setIsTourOpen(true);
    };

    const hideOnboardingTour = () => {
        setTourReshow(false);
        setIsTourOpen(false);
    };

    function handleKeyDown(e) {
        // Allow reshow if Cmnd + Ctrl + T is pressed
        if (e.metaKey && e.ctrlKey && e.key === 't') {
            showOnboardingTour();
        }
        if (e.metaKey && e.ctrlKey && e.key === 'h') {
            hideOnboardingTour();
        }
    }

    useEffect(() => {
        document.addEventListener('keydown', handleKeyDown);

        return () => document.removeEventListener('keydown', handleKeyDown);
    });

    return { showOnboardingTour };
};

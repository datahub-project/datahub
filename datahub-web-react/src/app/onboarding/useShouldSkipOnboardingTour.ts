/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

// this key is used in commands.js to turn off onboarding tours in cypress tests
export const SKIP_ONBOARDING_TOUR_KEY = 'skipOnboardingTour';

export default function useShouldSkipOnboardingTour() {
    const shouldSkipOnboardingTour = localStorage.getItem(SKIP_ONBOARDING_TOUR_KEY);

    return shouldSkipOnboardingTour === 'true';
}

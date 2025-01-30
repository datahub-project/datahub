// this key is used in commands.js to turn off onboarding tours in cypress tests
const SKIP_ONBOARDING_TOUR_KEY = 'skipOnboardingTour';

export default function useShouldSkipOnboardingTour() {
    const shouldSkipOnboardingTour = localStorage.getItem(SKIP_ONBOARDING_TOUR_KEY);

    return shouldSkipOnboardingTour === 'true';
}

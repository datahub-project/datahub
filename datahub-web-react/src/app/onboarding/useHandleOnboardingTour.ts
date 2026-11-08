import { useCallback, useContext, useEffect } from 'react';

import OnboardingContext from '@app/onboarding/OnboardingContext';

export const useHandleOnboardingTour = () => {
    const { setTourReshow, setIsTourOpen } = useContext(OnboardingContext);

    const showOnboardingTour = useCallback(() => {
        setTourReshow(true);
        setIsTourOpen(true);
    }, [setTourReshow, setIsTourOpen]);

    const hideOnboardingTour = useCallback(() => {
        setTourReshow(false);
        setIsTourOpen(false);
    }, [setTourReshow, setIsTourOpen]);

    // useState setters are stable, so this subscribes once per mount. NavSidebar
    // re-renders on every navigation; an effect with no dependency list was
    // removing and re-adding the document listener each time.
    useEffect(() => {
        function handleKeyDown(e: KeyboardEvent) {
            // Allow reshow if Cmnd + Ctrl + T is pressed
            if (e.metaKey && e.ctrlKey && e.key === 't') {
                showOnboardingTour();
            }
            if (e.metaKey && e.ctrlKey && e.key === 'h') {
                hideOnboardingTour();
            }
        }

        document.addEventListener('keydown', handleKeyDown);
        return () => document.removeEventListener('keydown', handleKeyDown);
    }, [showOnboardingTour, hideOnboardingTour]);

    return { showOnboardingTour };
};

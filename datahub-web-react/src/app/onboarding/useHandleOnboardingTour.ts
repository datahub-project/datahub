import { useContext, useEffect } from 'react';
import OnboardingContext from './OnboardingContext';

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

import React from 'react';

interface Props {
    tourReshow: boolean;
    setTourReshow: React.Dispatch<React.SetStateAction<boolean>>;
    isTourOpen: boolean;
    setIsTourOpen: React.Dispatch<React.SetStateAction<boolean>>;
    isUserInitializing: boolean;
    setIsUserInitializing: React.Dispatch<React.SetStateAction<boolean>>;
}

const OnboardingContext = React.createContext<Props>({
    tourReshow: false,
    setTourReshow: () => {},
    isTourOpen: false,
    setIsTourOpen: () => {},
    isUserInitializing: false,
    setIsUserInitializing: () => {},
});

export default OnboardingContext;

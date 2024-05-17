import React, { useState } from 'react';
import OnboardingContext from './OnboardingContext';

export const OnboardingContextProvider = ({ children }: { children: React.ReactNode }) => {
    const [isOpen, setIsOpen] = useState(false);
    const [reshow, setReshow] = useState(false);
    const [isNewlyIntroducedUser, setIsNewlyIntroducedUser] = useState(false);

    return (
        <OnboardingContext.Provider
            value={{
                isTourOpen: isOpen,
                setIsTourOpen: setIsOpen,
                tourReshow: reshow,
                setTourReshow: setReshow,
                isUserInitializing: isNewlyIntroducedUser,
                setIsUserInitializing: setIsNewlyIntroducedUser,
            }}
        >
            {children}
        </OnboardingContext.Provider>
    );
};

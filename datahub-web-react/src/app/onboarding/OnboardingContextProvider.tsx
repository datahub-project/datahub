import React, { useState } from 'react';
import OnboardingContext from './OnboardingContext';

export const OnboardingContextProvider = ({ children }: { children: React.ReactNode }) => {
    const [isOpen, setIsOpen] = useState(false);
    const [reshow, setReshow] = useState(false);

    return (
        <OnboardingContext.Provider
            value={{
                isTourOpen: isOpen,
                setIsTourOpen: setIsOpen,
                tourReshow: reshow,
                setTourReshow: setReshow,
            }}
        >
            {children}
        </OnboardingContext.Provider>
    );
};

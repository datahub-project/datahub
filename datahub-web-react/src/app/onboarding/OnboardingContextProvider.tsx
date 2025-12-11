/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React, { useState } from 'react';

import OnboardingContext from '@app/onboarding/OnboardingContext';

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

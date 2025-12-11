/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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

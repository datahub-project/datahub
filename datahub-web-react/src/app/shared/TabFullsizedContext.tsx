/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React, { Dispatch, SetStateAction } from 'react';

interface EntityHeaderContextProps {
    setTabFullsize?: Dispatch<SetStateAction<boolean>>; // If undefined, isTabFullsize is fixed
    isTabFullsize: boolean;
}

// context controlling the main entity header on entity full screen pages.
const TabFullsizedContext = React.createContext<EntityHeaderContextProps>({
    isTabFullsize: false,
});

export default TabFullsizedContext;

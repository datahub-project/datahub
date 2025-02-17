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

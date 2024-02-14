import React, { Dispatch, SetStateAction } from 'react';

interface EntityHeaderContextProps {
    setTabFullsize: Dispatch<SetStateAction<boolean>>;
    isTabFullsize: boolean;
}

// context controlling the main entity header on entity full screen pages.
const TabFullsizedContext = React.createContext<EntityHeaderContextProps>({
    setTabFullsize: () => {},
    isTabFullsize: false,
});

export default TabFullsizedContext;

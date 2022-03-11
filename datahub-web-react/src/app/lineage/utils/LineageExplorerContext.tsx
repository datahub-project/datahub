import React from 'react';

export const LineageExplorerContext = React.createContext<LineageExplorerContextType>({
    expandTitles: false,
});

type LineageExplorerContextType = {
    expandTitles: boolean;
};

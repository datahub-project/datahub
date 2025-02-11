import React from 'react';

interface SearchCardContextType {
    showRemovalFromList: boolean;
}

const defaultContext: SearchCardContextType = {
    showRemovalFromList: false,
};

export const SearchCardContext = React.createContext<SearchCardContextType>(defaultContext);

export function useSearchCardContext() {
    return React.useContext(SearchCardContext);
}

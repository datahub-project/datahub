import React from 'react';

interface SearchCardContextType {
    showRemovalFromList: boolean;
    onRemove?: () => void;
    removeText?: string;
}

const defaultContext: SearchCardContextType = {
    showRemovalFromList: false,
    removeText: '',
};

export const SearchCardContext = React.createContext<SearchCardContextType>(defaultContext);

export function useSearchCardContext() {
    return React.useContext(SearchCardContext);
}

import { createContext, useContext } from 'react';

type SearchContextValue = {
    query: string | null;
};

export const SearchContext = createContext<SearchContextValue | null>(null);

export const useSearchContext = () => {
    const context = useContext(SearchContext);
    if (!context) throw new Error(`${useSearchContext.name} must be used under a SearchContext.Provider`);
    return context;
};

export const useSearchQuery = () => {
    return useSearchContext().query;
};

export default SearchContext;

import React, { useContext } from 'react';
import { QuickFilter } from '../types.generated';

interface AppStateType {
    quickFilters: QuickFilter[] | null;
    setQuickFilters: React.Dispatch<React.SetStateAction<QuickFilter[] | null>>;
    selectedQuickFilter: QuickFilter | null;
    setSelectedQuickFilter: React.Dispatch<React.SetStateAction<QuickFilter | null>>;
}

export const AppStateContext = React.createContext<AppStateType>({
    quickFilters: [],
    setQuickFilters: () => {},
    selectedQuickFilter: null,
    setSelectedQuickFilter: () => {},
});

export function useAppStateContext() {
    return useContext(AppStateContext);
}

export default AppStateContext;

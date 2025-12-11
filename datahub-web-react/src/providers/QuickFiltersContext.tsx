/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React, { useContext } from 'react';

import { QuickFilter } from '@types';

interface AppStateType {
    quickFilters: QuickFilter[] | null;
    setQuickFilters: React.Dispatch<React.SetStateAction<QuickFilter[] | null>>;
    selectedQuickFilter: QuickFilter | null;
    setSelectedQuickFilter: React.Dispatch<React.SetStateAction<QuickFilter | null>>;
}

export const QuickFiltersContext = React.createContext<AppStateType>({
    quickFilters: [],
    setQuickFilters: () => {},
    selectedQuickFilter: null,
    setSelectedQuickFilter: () => {},
});

export function useQuickFiltersContext() {
    return useContext(QuickFiltersContext);
}

export default QuickFiltersContext;

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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

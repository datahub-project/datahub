import React, { useContext, useMemo, useState } from 'react';

import { DEFAULT_STATUS_FILTER, DocumentStatusFilter } from '@app/document/utils/documentTreeFilters';

/**
 * DocumentFiltersContext - Shared filter state for the Documents sidebar.
 *
 * The sidebar owns the filter UI (Status select + Author multi-select + Source
 * multi-select). Filter state lives in a context above the sidebar so that
 * future surfaces (e.g. a Documents homepage) can also read/write the same
 * selection without prop-drilling.
 */

export interface DocumentFiltersContextValue {
    status: DocumentStatusFilter;
    selectedAuthorUrns: string[];
    selectedPlatformUrns: string[];

    setStatus: (status: DocumentStatusFilter) => void;
    setSelectedAuthorUrns: (urns: string[]) => void;
    setSelectedPlatformUrns: (urns: string[]) => void;
}

const DocumentFiltersContext = React.createContext<DocumentFiltersContextValue | undefined>(undefined);

export const useDocumentFilters = (): DocumentFiltersContextValue => {
    const ctx = useContext(DocumentFiltersContext);
    if (!ctx) {
        /* untranslated-text -- developer-facing error for hook misuse, not user-visible */
        throw new Error('useDocumentFilters must be used within DocumentFiltersProvider');
    }
    return ctx;
};

export const DocumentFiltersProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => {
    const [status, setStatus] = useState<DocumentStatusFilter>(DEFAULT_STATUS_FILTER);
    const [selectedAuthorUrns, setSelectedAuthorUrns] = useState<string[]>([]);
    const [selectedPlatformUrns, setSelectedPlatformUrns] = useState<string[]>([]);

    const value = useMemo<DocumentFiltersContextValue>(
        () => ({
            status,
            selectedAuthorUrns,
            selectedPlatformUrns,
            setStatus,
            setSelectedAuthorUrns,
            setSelectedPlatformUrns,
        }),
        [status, selectedAuthorUrns, selectedPlatformUrns],
    );

    return <DocumentFiltersContext.Provider value={value}>{children}</DocumentFiltersContext.Provider>;
};

import React, { createContext, useContext, useMemo, useState } from 'react';

import { DomainOwnerInfo } from '@app/domainV2/nestedDomains/domainSidebarFilters/domainSidebarFilters.utils';

interface DomainSidebarFiltersContextValue {
    /** URNs of owners the user has selected from the multi-select. Empty = no filter. */
    selectedOwnerUrns: string[];
    setSelectedOwnerUrns: (urns: string[]) => void;
    /**
     * Distinct owners that exist anywhere in the server-side domain index,
     * sourced from the `owners` facet aggregations on the root scroll query.
     * Populated by `DomainNavigator` whenever the server returns a new set of
     * facets; the dropdown reads it directly.
     */
    availableOwners: DomainOwnerInfo[];
    setAvailableOwners: (owners: DomainOwnerInfo[]) => void;
}

// Noop defaults (rather than a throw-on-missing-provider hook like
// `useDocumentFilters` uses) are intentional: `DomainNode` and
// `DomainNavigator` are also rendered inside picker variants
// (CreateDomainModal, DomainParentSelect, PolicyPrivilegeForm) that have no
// provider in their tree. Noop defaults keep those pickers working without
// wrapping every call site, while the real sidebar still flows through the
// provider below.
const DomainSidebarFiltersContext = createContext<DomainSidebarFiltersContextValue>({
    selectedOwnerUrns: [],
    setSelectedOwnerUrns: () => {},
    availableOwners: [],
    setAvailableOwners: () => {},
});

export function DomainSidebarFiltersProvider({ children }: { children: React.ReactNode }) {
    const [selectedOwnerUrns, setSelectedOwnerUrns] = useState<string[]>([]);
    const [availableOwners, setAvailableOwners] = useState<DomainOwnerInfo[]>([]);

    const value = useMemo<DomainSidebarFiltersContextValue>(
        () => ({
            selectedOwnerUrns,
            setSelectedOwnerUrns,
            availableOwners,
            setAvailableOwners,
        }),
        [selectedOwnerUrns, availableOwners],
    );

    return <DomainSidebarFiltersContext.Provider value={value}>{children}</DomainSidebarFiltersContext.Provider>;
}

export function useDomainSidebarFilters(): DomainSidebarFiltersContextValue {
    return useContext(DomainSidebarFiltersContext);
}

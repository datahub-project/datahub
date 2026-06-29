import React, { createContext, useCallback, useContext, useMemo, useRef, useState } from 'react';

import { DomainOwnerInfo } from '@app/domainV2/nestedDomains/domainSidebarFilters/domainSidebarFilters.utils';

interface DomainSidebarFiltersContextValue {
    /** URNs of owners the user has selected from the multi-select. Empty = no filter. */
    selectedOwnerUrns: string[];
    setSelectedOwnerUrns: (urns: string[]) => void;
    /**
     * Distinct owners discovered across all loaded domains, in insertion order
     * (first occurrence wins) — drives the options list for the Owner filter.
     */
    availableOwners: DomainOwnerInfo[];
    /**
     * Called by tree nodes as they render to publish the owners they know
     * about. The context dedupes by URN; calling repeatedly with the same
     * owner is a no-op. Designed to be safe to call from a render-phase
     * useEffect.
     */
    registerOwners: (owners: DomainOwnerInfo[]) => void;
}

// Noop defaults (rather than a throw-on-missing-provider hook like
// `useDocumentFilters` uses) are intentional: `DomainNode` calls
// `useDomainSidebarFilters()` unconditionally, but the node is also rendered
// inside picker variants (CreateDomainModal, DomainParentSelect,
// PolicyPrivilegeForm) that have no provider in their tree. Noop defaults
// keep those pickers working without wrapping every call site, while the
// real sidebar still flows through the provider below.
const DomainSidebarFiltersContext = createContext<DomainSidebarFiltersContextValue>({
    selectedOwnerUrns: [],
    setSelectedOwnerUrns: () => {},
    availableOwners: [],
    registerOwners: () => {},
});

export function DomainSidebarFiltersProvider({ children }: { children: React.ReactNode }) {
    const [selectedOwnerUrns, setSelectedOwnerUrns] = useState<string[]>([]);

    // Two-tier storage:
    //   - `seenUrnsRef` is a Set used inside `registerOwners` to dedupe
    //     without scheduling a re-render when the URN was already known.
    //   - `availableOwners` is the rendered, ordered list — only updated
    //     when at least one URN in the incoming batch is new.
    // Doing it this way keeps the registration call O(1) per owner in the
    // common case (re-renders that don't add new owners), which matters
    // because `registerOwners` is called from every DomainNode on every
    // render-phase effect.
    const seenUrnsRef = useRef<Set<string>>(new Set());
    const [availableOwners, setAvailableOwners] = useState<DomainOwnerInfo[]>([]);

    const registerOwners = useCallback((owners: DomainOwnerInfo[]) => {
        if (owners.length === 0) return;
        const newcomers: DomainOwnerInfo[] = [];
        owners.forEach((owner) => {
            if (!seenUrnsRef.current.has(owner.urn)) {
                seenUrnsRef.current.add(owner.urn);
                newcomers.push(owner);
            }
        });
        if (newcomers.length > 0) {
            setAvailableOwners((current) => [...current, ...newcomers]);
        }
    }, []);

    const value = useMemo<DomainSidebarFiltersContextValue>(
        () => ({
            selectedOwnerUrns,
            setSelectedOwnerUrns,
            availableOwners,
            registerOwners,
        }),
        [selectedOwnerUrns, availableOwners, registerOwners],
    );

    return <DomainSidebarFiltersContext.Provider value={value}>{children}</DomainSidebarFiltersContext.Provider>;
}

export function useDomainSidebarFilters(): DomainSidebarFiltersContextValue {
    return useContext(DomainSidebarFiltersContext);
}

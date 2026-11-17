import React, { createContext, useContext, useMemo, useState } from 'react';

import { FacetSelectOption } from '@app/document/utils/documentSidebarFacets.utils';
import { DomainOwnerInfo } from '@app/domainV2/nestedDomains/domainSidebarFilters/domainSidebarFilters.utils';
import {
    DEFAULT_GLOSSARY_SIDEBAR_SORT,
    GlossarySidebarSortValue,
} from '@app/glossaryV2/glossarySidebarFilters/glossarySidebarSort';

interface GlossarySidebarFiltersContextValue {
    selectedOwnerUrns: string[];
    setSelectedOwnerUrns: (urns: string[]) => void;
    availableOwners: DomainOwnerInfo[];
    setAvailableOwners: (owners: DomainOwnerInfo[]) => void;
    selectedTagUrns: string[];
    setSelectedTagUrns: (urns: string[]) => void;
    /** Same shape as Documents sidebar — keeps `entity` for DomainLink / TagLink pills. */
    availableTags: FacetSelectOption[];
    setAvailableTags: (tags: FacetSelectOption[]) => void;
    selectedDomainUrns: string[];
    setSelectedDomainUrns: (urns: string[]) => void;
    availableDomains: FacetSelectOption[];
    setAvailableDomains: (domains: FacetSelectOption[]) => void;
    sortSelection: GlossarySidebarSortValue;
    setSortSelection: (sort: GlossarySidebarSortValue) => void;
}

// Noop defaults: GlossaryBrowser is also used in picker variants without a provider.
const GlossarySidebarFiltersContext = createContext<GlossarySidebarFiltersContextValue>({
    selectedOwnerUrns: [],
    setSelectedOwnerUrns: () => {},
    availableOwners: [],
    setAvailableOwners: () => {},
    selectedTagUrns: [],
    setSelectedTagUrns: () => {},
    availableTags: [],
    setAvailableTags: () => {},
    selectedDomainUrns: [],
    setSelectedDomainUrns: () => {},
    availableDomains: [],
    setAvailableDomains: () => {},
    sortSelection: DEFAULT_GLOSSARY_SIDEBAR_SORT,
    setSortSelection: () => {},
});

export function GlossarySidebarFiltersProvider({ children }: { children: React.ReactNode }) {
    const [selectedOwnerUrns, setSelectedOwnerUrns] = useState<string[]>([]);
    const [availableOwners, setAvailableOwners] = useState<DomainOwnerInfo[]>([]);
    const [selectedTagUrns, setSelectedTagUrns] = useState<string[]>([]);
    const [availableTags, setAvailableTags] = useState<FacetSelectOption[]>([]);
    const [selectedDomainUrns, setSelectedDomainUrns] = useState<string[]>([]);
    const [availableDomains, setAvailableDomains] = useState<FacetSelectOption[]>([]);
    const [sortSelection, setSortSelection] = useState<GlossarySidebarSortValue>(DEFAULT_GLOSSARY_SIDEBAR_SORT);

    const value = useMemo<GlossarySidebarFiltersContextValue>(
        () => ({
            selectedOwnerUrns,
            setSelectedOwnerUrns,
            availableOwners,
            setAvailableOwners,
            selectedTagUrns,
            setSelectedTagUrns,
            availableTags,
            setAvailableTags,
            selectedDomainUrns,
            setSelectedDomainUrns,
            availableDomains,
            setAvailableDomains,
            sortSelection,
            setSortSelection,
        }),
        [
            selectedOwnerUrns,
            availableOwners,
            selectedTagUrns,
            availableTags,
            selectedDomainUrns,
            availableDomains,
            sortSelection,
        ],
    );

    return <GlossarySidebarFiltersContext.Provider value={value}>{children}</GlossarySidebarFiltersContext.Provider>;
}

export function useGlossarySidebarFilters(): GlossarySidebarFiltersContextValue {
    return useContext(GlossarySidebarFiltersContext);
}

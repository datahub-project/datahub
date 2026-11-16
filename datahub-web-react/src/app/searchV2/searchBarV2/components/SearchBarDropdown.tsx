import React, { useEffect, useRef } from 'react';
import styled from 'styled-components';

import { AppliedFieldFilterUpdater, FieldToAppliedFieldFiltersMap } from '@app/searchV2/filtersV2/types';
import AutocompleteFooter from '@app/searchV2/searchBarV2/components/AutocompleteFooter';
import Filters from '@app/searchV2/searchBarV2/components/Filters';
import { radius } from '@src/alchemy-components';
import { FacetMetadata } from '@src/types.generated';

const DropdownContainer = styled.div`
    overflow: auto;
    box-shadow: ${(props) => props.theme.colors.shadowXl};
    border-radius: ${radius.lg};
    background: ${(props) => props.theme.colors.bg};
`;

// antd's Select (rc-select 14) renders each option row with aria-selected but no role, and gives group
// headings in its screen-reader list role="presentation" plus aria-selected. Both are invalid ARIA, and
// neither can be changed through the options, so they're corrected on the rendered menu.
function fixSelectOptionAria(root: HTMLElement) {
    root.querySelectorAll('.ant-select-item-option:not([role])').forEach((el) => el.setAttribute('role', 'option'));
    root.querySelectorAll('[role="presentation"][aria-selected]').forEach((el) => el.removeAttribute('aria-selected'));
}

interface Props {
    menu: React.ReactNode;
    query: string;
    filters?: FieldToAppliedFieldFiltersMap;
    updateFilters?: AppliedFieldFilterUpdater;
    facets?: FacetMetadata[];
    isSearching?: boolean;
}

export default function SearchBarDropdown({ menu, query, filters, updateFilters, facets, isSearching }: Props) {
    const containerRef = useRef<HTMLDivElement>(null);

    useEffect(() => {
        const root = containerRef.current;
        if (!root) return undefined;
        fixSelectOptionAria(root);
        const observer = new MutationObserver(() => fixSelectOptionAria(root));
        observer.observe(root, {
            subtree: true,
            childList: true,
            attributes: true,
            attributeFilter: ['aria-selected'],
        });
        return () => observer.disconnect();
    }, []);

    return (
        <DropdownContainer ref={containerRef} data-testid="search-bar-dropdown">
            {isSearching && (
                <Filters
                    query={query ?? ''}
                    appliedFilters={filters}
                    updateFieldAppliedFilters={updateFilters}
                    facets={facets}
                />
            )}
            {menu}
            <AutocompleteFooter isSomethingSelected={!!query} />
        </DropdownContainer>
    );
}

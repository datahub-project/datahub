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

// antd's Select (rc-select 14) keeps the accessible listbox in a hidden copy of the list, and the
// visible rows are only for the pointer. It still puts aria-selected on those visible rows, which have
// no role, and on group headings in the hidden list, which are role="presentation". Both are invalid
// ARIA and neither can be changed through the options, so it's removed from the rendered menu. The
// hidden listbox's own options keep it.
function fixSelectOptionAria(root: HTMLElement) {
    root.querySelectorAll('.ant-select-item-option[aria-selected], [role="presentation"][aria-selected]').forEach(
        (el) => el.removeAttribute('aria-selected'),
    );
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

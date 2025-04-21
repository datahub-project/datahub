import React from 'react';
import styled from 'styled-components';

import { AppliedFieldFilterUpdater, FieldToAppliedFieldFiltersMap } from '@app/searchV2/filtersV2/types';
import AutocompleteFooter from '@app/searchV2/searchBarV2/components/AutocompleteFooter';
import Filters from '@app/searchV2/searchBarV2/components/Filters';
import { BOX_SHADOW } from '@app/searchV2/searchBarV2/constants';
import { colors, radius } from '@src/alchemy-components';
import { FacetMetadata } from '@src/types.generated';

const DropdownContainer = styled.div`
    overflow: auto;
    box-shadow: ${BOX_SHADOW};
    border-radius: ${radius.lg};
    background: ${colors.white};
`;

interface Props {
    menu: React.ReactNode;
    query: string;
    filters?: FieldToAppliedFieldFiltersMap;
    updateFilters?: AppliedFieldFilterUpdater;
    facets?: FacetMetadata[];
    isSearching?: boolean;
}

export default function SearchBarDropdown({ menu, query, filters, updateFilters, facets, isSearching }: Props) {
    return (
        <DropdownContainer>
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

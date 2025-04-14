import React from 'react';
import { AppliedFieldFilterUpdater, FieldToAppliedFieldFiltersMap } from '../../filtersV2/types';
import { FacetMetadata } from '@src/types.generated';
import AutocompleteFooter from './AutocompleteFooter';
import Filters from './Filters';
import styled from 'styled-components';
import { colors, radius } from '@src/alchemy-components';
import { BOX_SHADOW } from '../constants';

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

export default function AutocompleteDropdown({ menu, query, filters, updateFilters, facets, isSearching }: Props) {
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

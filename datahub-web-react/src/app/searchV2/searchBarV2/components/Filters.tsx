import React, { memo, useMemo } from 'react';
import styled from 'styled-components';

import { useUserContext } from '@app/context/useUserContext';
import SearchFilters from '@app/searchV2/filtersV2/SearchFilters';
import DefaultFiltersRenderer from '@app/searchV2/filtersV2/defaults/DefaultFiltersRenderer';
import {
    AppliedFieldFilterUpdater,
    FieldToAppliedFieldFiltersMap,
    FiltersRendererProps,
} from '@app/searchV2/filtersV2/types';
import { convertFacetsToFieldToFacetStateMap } from '@app/searchV2/filtersV2/utils';
import {
    DOMAINS_FILTER_NAME,
    ENTITY_SUB_TYPE_FILTER_NAME,
    OWNERS_FILTER_NAME,
    PLATFORM_FILTER_NAME,
    TAGS_FILTER_NAME,
} from '@app/searchV2/utils/constants';
import { FacetMetadata } from '@src/types.generated';

const FILTER_FIELDS = [
    PLATFORM_FILTER_NAME,
    ENTITY_SUB_TYPE_FILTER_NAME,
    OWNERS_FILTER_NAME,
    TAGS_FILTER_NAME,
    DOMAINS_FILTER_NAME,
];

const Container = styled.div`
    padding: 16px 0;
    margin: 0 8px;
    overflow-x: auto;
    /* Hide scrollbar for Chrome, Safari, and Opera */
    &::-webkit-scrollbar {
        display: none;
    }
`;

function FiltersRenderer({ filters }: FiltersRendererProps) {
    if (filters.length === 0) return null;

    return (
        <Container>
            <DefaultFiltersRenderer filters={filters} />
        </Container>
    );
}

const MemoFiltersRenderer = memo(FiltersRenderer);

interface Props {
    query: string;
    appliedFilters?: FieldToAppliedFieldFiltersMap;
    updateFieldAppliedFilters?: AppliedFieldFilterUpdater;
    facets?: FacetMetadata[];
}

export default function Filters({ query, appliedFilters, updateFieldAppliedFilters, facets }: Props) {
    const userContext = useUserContext();
    const viewUrn = userContext.localState?.selectedViewUrn;
    const fieldToFacetStateMap = useMemo(() => convertFacetsToFieldToFacetStateMap(facets), [facets]);

    return (
        <SearchFilters
            fields={FILTER_FIELDS}
            query={query}
            viewUrn={viewUrn}
            appliedFilters={appliedFilters}
            updateFieldAppliedFilters={updateFieldAppliedFilters}
            filtersRenderer={MemoFiltersRenderer}
            fieldToFacetStateMap={fieldToFacetStateMap}
            shouldUpdateFacetsForFieldsWithAppliedFilters
            shouldUpdateFacetsForFieldsWithoutAppliedFilters={fieldToFacetStateMap === undefined}
        />
    );
}

import React, { memo } from 'react';
import styled from 'styled-components';

import SearchFilters from '@app/searchV2/filtersV2/SearchFilters';
import DefaultFiltersRenderer from '@app/searchV2/filtersV2/defaults/DefaultFiltersRenderer';
import {
    AppliedFieldFilterUpdater,
    FieldToAppliedFieldFiltersMap,
    FiltersRendererProps,
} from '@app/searchV2/filtersV2/types';
import {
    DOMAINS_FILTER_NAME,
    ENTITY_SUB_TYPE_FILTER_NAME,
    OWNERS_FILTER_NAME,
    PLATFORM_FILTER_NAME,
    TAGS_FILTER_NAME,
} from '@app/searchV2/utils/constants';

const FILTER_FIELDS = [
    PLATFORM_FILTER_NAME,
    ENTITY_SUB_TYPE_FILTER_NAME,
    OWNERS_FILTER_NAME,
    TAGS_FILTER_NAME,
    DOMAINS_FILTER_NAME,
];

const Container = styled.div`
    padding: 16px 8px;
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
}

export default function Filters({ query, appliedFilters, updateFieldAppliedFilters }: Props) {
    return (
        <SearchFilters
            fields={FILTER_FIELDS}
            query={query}
            appliedFilters={appliedFilters}
            updateFieldAppliedFilters={updateFieldAppliedFilters}
            filtersRenderer={MemoFiltersRenderer}
        />
    );
}

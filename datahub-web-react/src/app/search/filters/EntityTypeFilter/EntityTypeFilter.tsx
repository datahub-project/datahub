import React, { useEffect, useState } from 'react';
import { FacetFilterInput, FacetMetadata } from '../../../../types.generated';
import { useAggregateAcrossEntitiesLazyQuery } from '../../../../graphql/search.generated';
import useGetSearchQueryInputs from '../../useGetSearchQueryInputs';
import {
    ENTITY_FILTER_NAME,
    ENTITY_SUB_TYPE_FILTER_FIELDS,
    ENTITY_SUB_TYPE_FILTER_NAME,
    LEGACY_ENTITY_FILTER_FIELDS,
} from '../../utils/constants';
import { getFilterDropdownIcon, getNewFilters } from '../utils';
import { FilterOptionType } from '../types';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { getDisplayedFilterOptions, getInitialSelectedOptions, getNumActiveFilters } from './entityTypeFilterUtils';
import SearchFilterView from '../SearchFilterView';

interface Props {
    filter: FacetMetadata;
    activeFilters: FacetFilterInput[];
    onChangeFilters: (newFilters: FacetFilterInput[]) => void;
}

export default function EntityTypeFilter({ filter, activeFilters, onChangeFilters }: Props) {
    const entityRegistry = useEntityRegistry();
    const [selectedFilterOptions, setSelectedFilterOptions] = useState<FilterOptionType[]>([]);
    const [searchQuery, setSearchQuery] = useState<string>('');
    const [isMenuOpen, setIsMenuOpen] = useState(false);
    const { query, orFilters, viewUrn } = useGetSearchQueryInputs(ENTITY_SUB_TYPE_FILTER_FIELDS);
    const [aggregateAcrossEntities, { data, loading }] = useAggregateAcrossEntitiesLazyQuery();

    useEffect(() => {
        setSelectedFilterOptions(getInitialSelectedOptions(activeFilters, data));
    }, [activeFilters, data]);

    function updateIsMenuOpen(isOpen: boolean) {
        setIsMenuOpen(isOpen);
        setSearchQuery('');

        if (isOpen) {
            aggregateAcrossEntities({
                variables: {
                    input: {
                        query,
                        orFilters,
                        viewUrn,
                        facets: [ENTITY_SUB_TYPE_FILTER_NAME, ENTITY_FILTER_NAME],
                    },
                },
            });
        }
    }

    function updateFilters() {
        const activeFiltersWithoutLegacyFacets = activeFilters.filter(
            (f) => !LEGACY_ENTITY_FILTER_FIELDS.includes(f.field),
        );
        onChangeFilters(
            getNewFilters(
                ENTITY_SUB_TYPE_FILTER_NAME,
                activeFiltersWithoutLegacyFacets,
                selectedFilterOptions.map((f) => f.value),
            ),
        );
        setIsMenuOpen(false);
    }

    const filterOptions = getDisplayedFilterOptions(
        selectedFilterOptions,
        entityRegistry,
        setSelectedFilterOptions,
        searchQuery,
        data,
    );
    const numActiveFilters = getNumActiveFilters(activeFilters);
    const filterIcon = getFilterDropdownIcon(filter.field);

    return (
        <SearchFilterView
            filterOptions={filterOptions}
            isMenuOpen={isMenuOpen}
            numActiveFilters={numActiveFilters}
            filterIcon={filterIcon}
            displayName={filter.displayName || ''}
            searchQuery={searchQuery}
            loading={loading}
            updateIsMenuOpen={updateIsMenuOpen}
            setSearchQuery={setSearchQuery}
            updateFilters={updateFilters}
        />
    );
}

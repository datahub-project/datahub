import { useMemo, useState } from 'react';
import {
    GetAutoCompleteMultipleResultsQuery,
    useAggregateAcrossEntitiesLazyQuery,
    useGetAutoCompleteMultipleResultsLazyQuery,
} from '../../../graphql/search.generated';
import { FacetFilterInput, FacetMetadata } from '../../../types.generated';
import { useEntityRegistry } from '../../useEntityRegistry';
import useGetSearchQueryInputs from '../useGetSearchQueryInputs';
import { ENTITY_FILTER_NAME } from '../utils/constants';
import { FACETS_TO_ENTITY_TYPES } from './constants';
import { mapFilterOption } from './mapFilterOption';
import { FilterOptionType } from './types';
import {
    combineAggregations,
    filterEmptyAggregations,
    filterOptionsWithSearch,
    getFilterOptions,
    getNewFilters,
    getNumActiveFiltersForFilter,
} from './utils';

interface Props {
    filter: FacetMetadata;
    activeFilters: FacetFilterInput[];
    onChangeFilters: (newFilters: FacetFilterInput[]) => void;
}

export default function useSearchFilterDropdown({ filter, activeFilters, onChangeFilters }: Props) {
    const entityRegistry = useEntityRegistry();
    const initialFilters = activeFilters.find((f) => f.field === filter.field)?.values;
    const initialFilterOptions = filter.aggregations
        .filter((agg) => initialFilters?.includes(agg.value))
        .map((agg) => ({ field: filter.field, value: agg.value, entity: agg.entity, count: agg.count }));
    const [selectedFilterOptions, setSelectedFilterOptions] = useState<FilterOptionType[]>(initialFilterOptions || []);
    const [isMenuOpen, setIsMenuOpen] = useState(false);
    const [searchQuery, setSearchQuery] = useState<string>('');
    const { entityFilters, query, orFilters, viewUrn } = useGetSearchQueryInputs(
        useMemo(() => [filter.field], [filter.field]),
    );
    const [aggregateAcrossEntities, { data, loading }] = useAggregateAcrossEntitiesLazyQuery();
    const [autoCompleteResults, setAutoCompleteResults] = useState<GetAutoCompleteMultipleResultsQuery | undefined>(
        undefined,
    );
    const [getAutoCompleteResults] = useGetAutoCompleteMultipleResultsLazyQuery({
        onCompleted: (result) => setAutoCompleteResults(result),
    });

    const numActiveFilters = getNumActiveFiltersForFilter(activeFilters, filter);

    function updateIsMenuOpen(isOpen: boolean) {
        setIsMenuOpen(isOpen);
        // set filters to default every time you open or close the menu without saving
        setSelectedFilterOptions(initialFilterOptions || []);
        setSearchQuery('');
        setAutoCompleteResults(undefined);

        if (isOpen && numActiveFilters > 0) {
            aggregateAcrossEntities({
                variables: {
                    input: {
                        types: filter.field === ENTITY_FILTER_NAME ? null : entityFilters,
                        query,
                        orFilters,
                        viewUrn,
                        facets: [filter.field],
                    },
                },
            });
        }
    }

    function updateFilters() {
        onChangeFilters(
            getNewFilters(
                filter.field,
                activeFilters,
                selectedFilterOptions.map((f) => f.value),
            ),
        );
        setIsMenuOpen(false);
    }

    function manuallyUpdateFilters(newFilters: FacetFilterInput[]) {
        // remove any filters that are in newFilters to overwrite them
        const filtersNotInNewFilters = activeFilters.filter(
            (f) => !newFilters.find((newFilter) => newFilter.field === f.field),
        );
        onChangeFilters([...filtersNotInNewFilters, ...newFilters]);
        setIsMenuOpen(false);
    }

    function updateSearchQuery(newQuery: string) {
        setSearchQuery(newQuery);
        if (newQuery && FACETS_TO_ENTITY_TYPES[filter.field]) {
            getAutoCompleteResults({
                variables: {
                    input: {
                        query: newQuery,
                        limit: 10,
                        types: FACETS_TO_ENTITY_TYPES[filter.field],
                    },
                },
            });
        }
        if (!newQuery) {
            setAutoCompleteResults(undefined);
        }
    }

    // combine existing aggs from search response with new aggregateAcrossEntities response
    const combinedAggregations = combineAggregations(
        filter.field,
        filter.aggregations,
        data?.aggregateAcrossEntities?.facets,
    );
    // filter out any aggregations with a count of 0 unless it's already selected and in activeFilters
    const finalAggregations = filterEmptyAggregations(combinedAggregations, activeFilters);
    const filterOptions = getFilterOptions(
        filter.field,
        finalAggregations,
        selectedFilterOptions,
        autoCompleteResults,
        filter.entity,
    )
        .map((filterOption) =>
            mapFilterOption({ filterOption, entityRegistry, selectedFilterOptions, setSelectedFilterOptions }),
        )
        .filter((option) => filterOptionsWithSearch(searchQuery, option.displayName as string));

    return {
        isMenuOpen,
        updateIsMenuOpen,
        updateFilters,
        filterOptions,
        numActiveFilters,
        areFiltersLoading: loading,
        searchQuery,
        updateSearchQuery,
        manuallyUpdateFilters,
    };
}

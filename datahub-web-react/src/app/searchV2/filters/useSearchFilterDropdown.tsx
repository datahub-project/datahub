import { FacetFilterInput, FacetMetadata } from '../../../types.generated';
import { getNewFilters, getNumActiveFiltersForFilter } from './utils';

interface Props {
    filter: FacetMetadata;
    activeFilters: FacetFilterInput[];
    onChangeFilters: (newFilters: FacetFilterInput[]) => void;
}

export default function useSearchFilterDropdown({ filter, activeFilters, onChangeFilters }: Props) {
    const numActiveFilters = getNumActiveFiltersForFilter(activeFilters, filter);

    function updateFilters(newFilters) {
        onChangeFilters(
            getNewFilters(
                filter.field,
                activeFilters,
                newFilters.map((f) => f.value),
            ),
        );
    }

    function manuallyUpdateFilters(newFilters: FacetFilterInput[]) {
        // remove any filters that are in newFilters to overwrite them
        const filtersNotInNewFilters = activeFilters.filter(
            (f) => !newFilters.find((newFilter) => newFilter.field === f.field),
        );
        onChangeFilters([...filtersNotInNewFilters, ...newFilters]);
    }

    return {
        updateFilters,
        numActiveFilters,
        manuallyUpdateFilters,
    };
}

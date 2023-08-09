import { useSelectedSortOption } from '../context/SearchContext';
import { RECOMMENDED, SORT_OPTIONS } from '../context/constants';

export default function useSortInput() {
    const selectedSortOption = useSelectedSortOption();

    // do not return a sortInput if the option is our default/recommended
    if (!selectedSortOption || selectedSortOption === RECOMMENDED) return undefined;

    const sortOption = selectedSortOption in SORT_OPTIONS ? SORT_OPTIONS[selectedSortOption] : null;

    return sortOption ? { sortCriterion: { field: sortOption.field, sortOrder: sortOption.sortOrder } } : undefined;
}

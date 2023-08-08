import { RECOMMENDED, SORT_OPTIONS } from '../context/constants';
import { useSearchContext } from '../context/useSearchContext';

export default function useSortInput() {
    const { selectedSortOption } = useSearchContext();

    // do not return a sortInput if the option is our default/recommended
    if (selectedSortOption === RECOMMENDED) return undefined;

    const sortOption = selectedSortOption in SORT_OPTIONS ? SORT_OPTIONS[selectedSortOption] : null;

    return sortOption ? { sortCriterion: { field: sortOption.field, sortOrder: sortOption.sortOrder } } : undefined;
}

import { useSelectedSortOption } from '@app/search/context/SearchContext';
import { RELEVANCE, getSortOptions } from '@app/search/context/constants';

export default function useSortInput() {
    const selectedSortOption = useSelectedSortOption();

    // do not return a sortInput if the option is our default/recommended
    if (!selectedSortOption || selectedSortOption === RELEVANCE) return undefined;

    const sortOptions = getSortOptions();
    const sortOption = selectedSortOption in sortOptions ? sortOptions[selectedSortOption] : null;

    return sortOption ? { sortCriterion: { field: sortOption.field, sortOrder: sortOption.sortOrder } } : undefined;
}

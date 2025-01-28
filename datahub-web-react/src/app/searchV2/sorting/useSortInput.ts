import { RELEVANCE } from '../context/constants';
import useGetSortOptions from './useGetSortOptions';

export default function useSortInput(selectedSortOption: string | undefined) {
    const sortOptions = useGetSortOptions();

    // do not return a sortInput if the option is our default/recommended
    if (!selectedSortOption || selectedSortOption === RELEVANCE) return undefined;

    const sortOption = selectedSortOption in sortOptions ? sortOptions[selectedSortOption] : null;

    return sortOption ? { sortCriterion: { field: sortOption.field, sortOrder: sortOption.sortOrder } } : undefined;
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useSelectedSortOption } from '@app/search/context/SearchContext';
import { RELEVANCE, SORT_OPTIONS } from '@app/search/context/constants';

export default function useSortInput() {
    const selectedSortOption = useSelectedSortOption();

    // do not return a sortInput if the option is our default/recommended
    if (!selectedSortOption || selectedSortOption === RELEVANCE) return undefined;

    const sortOption = selectedSortOption in SORT_OPTIONS ? SORT_OPTIONS[selectedSortOption] : null;

    return sortOption ? { sortCriterion: { field: sortOption.field, sortOrder: sortOption.sortOrder } } : undefined;
}

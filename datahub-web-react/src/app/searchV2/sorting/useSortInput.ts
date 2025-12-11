/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { RELEVANCE } from '@app/searchV2/context/constants';
import useGetSortOptions from '@app/searchV2/sorting/useGetSortOptions';

export default function useSortInput(selectedSortOption: string | undefined) {
    const sortOptions = useGetSortOptions();

    // do not return a sortInput if the option is our default/recommended
    if (!selectedSortOption || selectedSortOption === RELEVANCE) return undefined;

    const sortOption = selectedSortOption in sortOptions ? sortOptions[selectedSortOption] : null;

    return sortOption ? { sortCriterion: { field: sortOption.field, sortOrder: sortOption.sortOrder } } : undefined;
}

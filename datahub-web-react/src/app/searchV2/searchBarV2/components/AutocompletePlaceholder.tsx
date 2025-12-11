/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

import NoResultsFoundPlaceholder from '@app/searchV2/searchBarV2/components/NoResultsFoundPlaceholder';
import NoSearchingPlaceholder from '@app/searchV2/searchBarV2/components/NoSearchingPlaceholder';

interface Props {
    isSearching?: boolean;
    hasAppliedFilters?: boolean;
    hasSelectedView?: boolean;
    onClearFilters?: () => void;
}

export default function AutocompletePlaceholder({
    isSearching,
    hasAppliedFilters,
    hasSelectedView,
    onClearFilters,
}: Props) {
    if (isSearching) {
        return (
            <NoResultsFoundPlaceholder
                hasAppliedFilters={hasAppliedFilters}
                hasSelectedView={hasSelectedView}
                onClearFilters={onClearFilters}
            />
        );
    }

    return <NoSearchingPlaceholder />;
}

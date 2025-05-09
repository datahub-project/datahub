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

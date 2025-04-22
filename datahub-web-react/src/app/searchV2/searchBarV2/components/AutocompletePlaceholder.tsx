import React from 'react';

import NoResultsFoundPlaceholder from '@app/searchV2/searchBarV2/components/NoResultsFoundPlaceholder';
import NoSearchingPlaceholder from '@app/searchV2/searchBarV2/components/NoSearchingPlaceholder';

interface Props {
    isSearching?: boolean;
    hasAppliedFilters?: boolean;
    onClearFilters?: () => void;
}

export default function AutocompletePlaceholder({ isSearching, hasAppliedFilters, onClearFilters }: Props) {
    if (isSearching) {
        return <NoResultsFoundPlaceholder hasAppliedFilters={hasAppliedFilters} onClearFilters={onClearFilters} />;
    }

    return <NoSearchingPlaceholder />;
}

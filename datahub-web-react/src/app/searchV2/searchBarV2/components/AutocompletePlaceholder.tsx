import React from 'react';
<<<<<<< HEAD

import NoResultsFoundPlaceholder from '@app/searchV2/searchBarV2/components/NoResultsFoundPlaceholder';
import NoSearchingPlaceholder from '@app/searchV2/searchBarV2/components/NoSearchingPlaceholder';
=======
import NoResultsFoundPlaceholder from './NoResultsFoundPlaceholder';
import NoSearchingPlaceholder from './NoSearchingPlaceholder';
>>>>>>> dbad52283b070c7cc136306c1553770db2f72105

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

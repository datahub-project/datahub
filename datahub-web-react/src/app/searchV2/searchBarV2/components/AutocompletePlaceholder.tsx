import React from 'react';
import NoResultsFoundPlaceholder from './NoResultsFoundPlaceholder';
import NoSearchingPlaceholder from './NoSearchingPlaceholder';

interface Props {
    isSearching?: boolean;
    onClearFilters?: () => void;
}

export default function AutocompletePlaceholder({ isSearching, onClearFilters }: Props) {
    if (isSearching) return <NoResultsFoundPlaceholder onClearFilters={onClearFilters} />;

    return <NoSearchingPlaceholder />;
}

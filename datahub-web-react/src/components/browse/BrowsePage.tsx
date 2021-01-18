import * as React from 'react';
import * as QueryString from 'query-string';
import { useLocation } from 'react-router';
import { BrowseTypesPage } from './BrowseTypesPage';
import { BrowseResultsPage } from './BrowseResultsPage';

/**
 * Routes to a BrowseTypesPage, which serves as the entry point to the browse experience,
 * or to a BrowseResultsPage, which displays browse results.
 */
export const BrowsePage: React.FC = () => {
    const location = useLocation();
    const params = QueryString.parse(location.search);
    return <>{!params.type ? <BrowseTypesPage /> : <BrowseResultsPage />}</>;
};

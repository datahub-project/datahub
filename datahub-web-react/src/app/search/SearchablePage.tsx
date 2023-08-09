import React, { useEffect, useState } from 'react';
import { useHistory, useLocation } from 'react-router';
import * as QueryString from 'query-string';
import { useTheme } from 'styled-components';
import { SearchHeader } from './SearchHeader';
import { useEntityRegistry } from '../useEntityRegistry';
import { EntityType, FacetFilterInput } from '../../types.generated';
import {
    GetAutoCompleteMultipleResultsQuery,
    useGetAutoCompleteMultipleResultsLazyQuery,
} from '../../graphql/search.generated';
import { navigateToSearchUrl } from './utils/navigateToSearchUrl';
import analytics, { EventType } from '../analytics';
import useFilters from './utils/useFilters';
import { PageRoutes } from '../../conf/Global';
import { getAutoCompleteInputFromQuickFilter } from './utils/filterUtils';
import { useQuickFiltersContext } from '../../providers/QuickFiltersContext';
import { useUserContext } from '../context/useUserContext';
import { useSelectedSortOption } from './context/SearchContext';

const styles = {
    children: {
        flex: '1',
        marginTop: 60,
        display: 'flex',
        flexDirection: 'column' as const,
        maxHeight: 'calc(100vh - 60px)',
    },
};

interface Props extends React.PropsWithChildren<any> {
    onSearch?: (query: string, type?: EntityType) => void;
    onAutoComplete?: (query: string) => void;
}

const defaultProps = {
    onSearch: undefined,
    onAutoComplete: undefined,
};

const isSearchResultPage = (path: string) => {
    return path.startsWith(PageRoutes.SEARCH);
};

/**
 * A page that includes a sticky search header (nav bar)
 */
export const SearchablePage = ({ onSearch, onAutoComplete, children }: Props) => {
    const location = useLocation();
    const params = QueryString.parse(location.search, { arrayFormat: 'comma' });
    const paramFilters: Array<FacetFilterInput> = useFilters(params);
    const filters = isSearchResultPage(location.pathname) ? paramFilters : [];
    const currentQuery: string = isSearchResultPage(location.pathname)
        ? decodeURIComponent(params.query ? (params.query as string) : '')
        : '';
    const selectedSortOption = useSelectedSortOption();

    const history = useHistory();
    const entityRegistry = useEntityRegistry();
    const themeConfig = useTheme();
    const { selectedQuickFilter } = useQuickFiltersContext();

    const [getAutoCompleteResults, { data: suggestionsData }] = useGetAutoCompleteMultipleResultsLazyQuery();
    const userContext = useUserContext();
    const [newSuggestionData, setNewSuggestionData] = useState<GetAutoCompleteMultipleResultsQuery | undefined>();
    const { user } = userContext;
    const viewUrn = userContext.localState?.selectedViewUrn;

    useEffect(() => {
        if (suggestionsData !== undefined) {
            setNewSuggestionData(suggestionsData);
        }
    }, [suggestionsData]);

    const search = (query: string, type?: EntityType, quickFilters?: FacetFilterInput[]) => {
        analytics.event({
            type: EventType.SearchEvent,
            query,
            pageNumber: 1,
            originPath: window.location.pathname,
            selectedQuickFilterTypes: selectedQuickFilter ? [selectedQuickFilter.field] : undefined,
            selectedQuickFilterValues: selectedQuickFilter ? [selectedQuickFilter.value] : undefined,
        });

        const appliedFilters = quickFilters && quickFilters?.length > 0 ? quickFilters : filters;

        navigateToSearchUrl({
            type,
            query,
            filters: appliedFilters,
            history,
            selectedSortOption,
        });
    };

    const autoComplete = (query: string) => {
        if (query && query.trim() !== '') {
            getAutoCompleteResults({
                variables: {
                    input: {
                        query,
                        viewUrn,
                        ...getAutoCompleteInputFromQuickFilter(selectedQuickFilter),
                    },
                },
            });
        }
    };

    // Load correct autocomplete results on initial page load.
    useEffect(() => {
        if (currentQuery && currentQuery.trim() !== '') {
            getAutoCompleteResults({
                variables: {
                    input: {
                        query: currentQuery,
                        viewUrn,
                    },
                },
            });
        }
    }, [currentQuery, getAutoCompleteResults, viewUrn]);

    return (
        <>
            <SearchHeader
                initialQuery={currentQuery as string}
                placeholderText={themeConfig.content.search.searchbarMessage}
                suggestions={
                    (newSuggestionData &&
                        newSuggestionData?.autoCompleteForMultiple &&
                        newSuggestionData.autoCompleteForMultiple.suggestions) ||
                    []
                }
                onSearch={onSearch || search}
                onQueryChange={onAutoComplete || autoComplete}
                authenticatedUserUrn={user?.urn || ''}
                authenticatedUserPictureLink={user?.editableProperties?.pictureLink}
                entityRegistry={entityRegistry}
            />
            <div style={styles.children}>{children}</div>
        </>
    );
};

SearchablePage.defaultProps = defaultProps;

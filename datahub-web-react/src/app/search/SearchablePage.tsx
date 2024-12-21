import React, { useEffect, useState } from 'react';
import { useHistory, useLocation } from 'react-router';
import { debounce } from 'lodash';
import * as QueryString from 'query-string';
import { useTheme } from 'styled-components';
import { Helmet } from 'react-helmet-async';
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
import { HALF_SECOND_IN_MS } from '../entity/shared/tabs/Dataset/Queries/utils/constants';
import { useBrowserTitle } from '../shared/BrowserTabTitleContext';

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

    const { title, updateTitle } = useBrowserTitle();

    useEffect(() => {
        // Update the title only if it's not already set and there is a valid pathname
        if (!title && location.pathname) {
            const formattedPath = location.pathname
                .split('/')
                .filter((word) => word !== '')
                .map((rawWord) => {
                    // ie. personal-notifications -> Personal Notifications
                    const words = rawWord.split('-');
                    return words.map((word) => word.charAt(0).toUpperCase() + word.slice(1)).join(' ');
                })
                .join(' | ');

            if (formattedPath) {
                return updateTitle(formattedPath);
            }
        }

        // Clean up the title when the component unmounts
        return () => {
            updateTitle('');
        };
    }, [location.pathname, title, updateTitle]);

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

    const autoComplete = debounce((query: string) => {
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
    }, HALF_SECOND_IN_MS);

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
            <Helmet>
                <title>{title}</title>
            </Helmet>
            <div style={styles.children}>{children}</div>
        </>
    );
};

SearchablePage.defaultProps = defaultProps;

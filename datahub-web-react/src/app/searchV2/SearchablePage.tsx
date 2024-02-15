import React, { useEffect, useState } from 'react';
import { useHistory, useLocation } from 'react-router';
import { debounce } from 'lodash';
import * as QueryString from 'query-string';
import styled, { useTheme } from 'styled-components';
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
import { useSelectedSortOption } from '../search/context/SearchContext';
import { NavSidebar } from '../homeV2/layout/NavSidebar';

const Body = styled.div`
    display: flex;
    flex-direction: row;
    flex: 1;
    background-color: #f4f5f7;
`;

const Navigation = styled.div`
    z-index: 14;
`;

const Content = styled.div`
    z-index: 12;
    border-radius: 8px;
    margin-top: 60px;
    flex: 1;
    display: flex;
    flex-direction: column;
    max-height: calc(100vh - 60px);
    width: 100%;
    overflow: auto;
`;

const FIFTH_SECOND_IN_MS = 100;

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
    }, FIFTH_SECOND_IN_MS);

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
            <Body>
                <Navigation>
                    <NavSidebar />
                </Navigation>
                <Content>{children}</Content>
            </Body>
        </>
    );
};

SearchablePage.defaultProps = defaultProps;

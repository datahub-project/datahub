import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';
import React, { useEffect, useState } from 'react';
import { useHistory, useLocation } from 'react-router';
import { debounce } from 'lodash';
import * as QueryString from 'query-string';
import styled, { useTheme } from 'styled-components';
import { SearchHeader } from './SearchHeader';
import { useEntityRegistry } from '../useEntityRegistry';
import { FacetFilterInput } from '../../types.generated';
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
`;

const BodyBackground = styled.div`
    background-color: ${REDESIGN_COLORS.BACKGROUND};
    position: fixed;
    height: 100%;
    width: 100%;
    z-index: -2;
`;

const Navigation = styled.div`
    z-index: 200;
`;

const Content = styled.div`
    border-radius: 8px;
    margin-top: 72px;
    flex: 1;
    display: flex;
    flex-direction: column;
    max-height: calc(100vh - 72px);
    width: 100%;
    overflow: auto;
`;

const FIFTH_SECOND_IN_MS = 100;

type Props = React.PropsWithChildren<any>;

const isSearchResultPage = (path: string) => {
    return path.startsWith(PageRoutes.SEARCH);
};

/**
 * A page that includes a sticky search header (nav bar)
 */
export const SearchablePage = ({ children }: Props) => {
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
    const viewUrn = userContext.localState?.selectedViewUrn;

    useEffect(() => {
        if (suggestionsData !== undefined) {
            setNewSuggestionData(suggestionsData);
        }
    }, [suggestionsData]);

    const search = (query: string, quickFilters?: FacetFilterInput[]) => {
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
                onSearch={search}
                onQueryChange={autoComplete}
                entityRegistry={entityRegistry}
            />
            <BodyBackground />
            <Body>
                <Navigation>
                    <NavSidebar />
                </Navigation>
                <Content>{children}</Content>
            </Body>
        </>
    );
};

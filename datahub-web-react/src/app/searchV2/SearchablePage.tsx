import { debounce } from 'lodash';
import React, { useEffect, useState } from 'react';
import { useHistory } from 'react-router';
import styled, { useTheme } from 'styled-components';

import analytics, { EventType } from '@app/analytics';
import { useUserContext } from '@app/context/useUserContext';
import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';
import { NavSidebar } from '@app/homeV2/layout/NavSidebar';
import { NavSidebar as NavSidebarRedesign } from '@app/homeV2/layout/navBarRedesign/NavSidebar';
import { useSelectedSortOption } from '@app/search/context/SearchContext';
import { SearchHeader } from '@app/searchV2/SearchHeader';
import useQueryAndFiltersFromLocation from '@app/searchV2/useQueryAndFiltersFromLocation';
import { getAutoCompleteInputFromQuickFilter } from '@app/searchV2/utils/filterUtils';
import { navigateToSearchUrl } from '@app/searchV2/utils/navigateToSearchUrl';
import { useAppConfig } from '@app/useAppConfig';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { useShowNavBarRedesign } from '@app/useShowNavBarRedesign';
import { useQuickFiltersContext } from '@providers/QuickFiltersContext';
import { colors } from '@src/alchemy-components';

import {
    GetAutoCompleteMultipleResultsQuery,
    useGetAutoCompleteMultipleResultsLazyQuery,
} from '@graphql/search.generated';
import { FacetFilterInput } from '@types';

const Body = styled.div`
    display: flex;
    flex-direction: row;
    flex: 1;
`;

const BodyBackground = styled.div<{ $isShowNavBarRedesign?: boolean }>`
    background-color: ${(props) => (props.$isShowNavBarRedesign ? colors.gray[1600] : REDESIGN_COLORS.BACKGROUND)};
    position: fixed;
    height: 100%;
    width: 100%;
    z-index: -2;
`;

const Navigation = styled.div<{ $isShowNavBarRedesign?: boolean }>`
    z-index: ${(props) => (props.$isShowNavBarRedesign ? 0 : 200)};
`;

const Content = styled.div<{ $isShowNavBarRedesign?: boolean }>`
    border-radius: ${(props) =>
        props.$isShowNavBarRedesign ? props.theme.styles['border-radius-navbar-redesign'] : '8px'};
    margin-top: ${(props) => (props.$isShowNavBarRedesign ? '56px' : '72px')};
    ${(props) =>
        props.$isShowNavBarRedesign &&
        `
        padding: 11px 15px 11px 3px;
    `}
    flex: 1;
    display: flex;
    flex-direction: column;
    max-height: ${(props) => (props.$isShowNavBarRedesign ? 'calc(100vh - 56px)' : 'calc(100vh - 72px)')};
    width: 100%;
    overflow: ${(props) => (props.$isShowNavBarRedesign ? 'hidden' : 'auto')};
`;

const FIFTH_SECOND_IN_MS = 100;

type Props = React.PropsWithChildren<any>;

/**
 * A page that includes a sticky search header (nav bar)
 */
export const SearchablePage = ({ children }: Props) => {
    const appConfig = useAppConfig();
    const showSearchBarAutocompleteRedesign = appConfig.config.featureFlags?.showSearchBarAutocompleteRedesign;
    const { filters, query: currentQuery } = useQueryAndFiltersFromLocation();
    const selectedSortOption = useSelectedSortOption();
    const isShowNavBarRedesign = useShowNavBarRedesign();

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

    const search = (query: string, newFilters?: FacetFilterInput[]) => {
        analytics.event({
            type: EventType.SearchEvent,
            query,
            pageNumber: 1,
            originPath: window.location.pathname,
            selectedQuickFilterTypes: selectedQuickFilter ? [selectedQuickFilter.field] : undefined,
            selectedQuickFilterValues: selectedQuickFilter ? [selectedQuickFilter.value] : undefined,
        });

        let newAppliedFilters: FacetFilterInput[] | undefined = filters;

        // For the redesigned search bar we should always pass new filters even though they are empty
        if (showSearchBarAutocompleteRedesign || (newFilters && newFilters?.length > 0)) {
            newAppliedFilters = newFilters;
        }

        navigateToSearchUrl({
            query,
            filters: newAppliedFilters,
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
        if (!showSearchBarAutocompleteRedesign && currentQuery && currentQuery.trim() !== '') {
            getAutoCompleteResults({
                variables: {
                    input: {
                        query: currentQuery,
                        viewUrn,
                    },
                },
            });
        }
    }, [currentQuery, getAutoCompleteResults, viewUrn, showSearchBarAutocompleteRedesign]);

    const FinalNavBar = isShowNavBarRedesign ? NavSidebarRedesign : NavSidebar;

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
            <BodyBackground $isShowNavBarRedesign={isShowNavBarRedesign} />
            <Body>
                <Navigation $isShowNavBarRedesign={isShowNavBarRedesign}>
                    <FinalNavBar />
                </Navigation>
                <Content $isShowNavBarRedesign={isShowNavBarRedesign}>{children}</Content>
            </Body>
        </>
    );
};

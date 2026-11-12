import debounce from 'lodash/debounce';
import React, { useEffect, useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled, { useTheme } from 'styled-components';

import { useUserContext } from '@app/context/useUserContext';
import { NavSidebar } from '@app/homeV2/layout/NavSidebar';
import { useNavBarContext } from '@app/homeV2/layout/navBarRedesign/NavBarContext';
import { NavSidebar as NavSidebarRedesign } from '@app/homeV2/layout/navBarRedesign/NavSidebar';
import { SearchHeader } from '@app/searchV2/SearchHeader';
import useGoToSearchPage from '@app/searchV2/useGoToSearchPage';
import useQueryAndFiltersFromLocation from '@app/searchV2/useQueryAndFiltersFromLocation';
import { getAutoCompleteInputFromQuickFilter } from '@app/searchV2/utils/filterUtils';
import { PAGE_GAP_PX } from '@app/sharedV2/layouts/constants';
import ProductUpdates from '@app/sharedV2/product/update/ProductUpdates';
import { useAppConfig } from '@app/useAppConfig';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { useShowNavBarRedesign } from '@app/useShowNavBarRedesign';
import { useQuickFiltersContext } from '@providers/QuickFiltersContext';

import {
    GetAutoCompleteMultipleResultsQuery,
    useGetAutoCompleteMultipleResultsLazyQuery,
} from '@graphql/search.generated';

const Body = styled.div`
    display: flex;
    flex-direction: row;
    flex: 1;
`;

const BodyBackground = styled.div<{ $isShowNavBarRedesign?: boolean }>`
    background-color: ${(props) =>
        props.$isShowNavBarRedesign ? props.theme.colors.bgSurfaceNewNav : props.theme.colors.bgSurface};
    position: fixed;
    height: 100%;
    width: 100%;
    z-index: -2;
`;

const Navigation = styled.div<{ $isShowNavBarRedesign?: boolean }>`
    z-index: ${(props) => (props.$isShowNavBarRedesign ? 0 : 200)};
`;

/**
 * Redesign header: 8px top margin + 56px row with the 44px search box centered, so the box ends
 * 58px down. Starting the shell there (plus its PAGE_GAP_PX padding) puts every page that far below the box.
 */
const REDESIGN_SEARCH_BOX_BOTTOM_PX = 58;

const Content = styled.div<{
    $isShowNavBarRedesign?: boolean;
    $hideSearchBar?: boolean;
    $isNavBarCollapsed?: boolean;
}>`
    border-radius: ${(props) =>
        props.$isShowNavBarRedesign ? props.theme.styles['border-radius-navbar-redesign'] : '8px'};
    margin-top: ${(props) => (props.$isShowNavBarRedesign ? `${REDESIGN_SEARCH_BOX_BOTTOM_PX}px` : '72px')};
    ${(props) => props.$hideSearchBar && `margin-top: ${props.$isShowNavBarRedesign ? 0 : 6}px;`}
    ${(props) =>
        props.$isShowNavBarRedesign &&
        `
        padding: ${PAGE_GAP_PX}px;
    `}
    flex: 1;
    display: flex;
    flex-direction: column;
    max-height: ${(props) =>
        props.$isShowNavBarRedesign ? `calc(100vh - ${REDESIGN_SEARCH_BOX_BOTTOM_PX}px)` : 'calc(100vh - 72px)'};
    max-height: ${(props) => props.$hideSearchBar && `calc(100vh - ${props.$isShowNavBarRedesign ? 0 : 6}px)`};
    width: 100%;
    overflow: ${(props) => (props.$isShowNavBarRedesign ? 'hidden' : 'auto')};
`;

const FIFTH_SECOND_IN_MS = 200;

type Props = React.PropsWithChildren<{
    hideSearchBar?: boolean;
}>;

/**
 * A page that includes a sticky search header (nav bar)
 */
export const SearchablePage = ({ children, hideSearchBar }: Props) => {
    const appConfig = useAppConfig();
    const showSearchBarAutocompleteRedesign = appConfig.config.featureFlags?.showSearchBarAutocompleteRedesign;
    const { query: currentQuery } = useQueryAndFiltersFromLocation();
    const isShowNavBarRedesign = useShowNavBarRedesign();
    const { isCollapsed } = useNavBarContext();

    const entityRegistry = useEntityRegistry();
    const themeConfig = useTheme();
    const { t } = useTranslation('search');
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

    const search = useGoToSearchPage(selectedQuickFilter);

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
                placeholderText={t('searchBar.placeholder', {
                    defaultValue: themeConfig.content.search.searchbarMessage,
                })}
                suggestions={
                    (newSuggestionData &&
                        newSuggestionData?.autoCompleteForMultiple &&
                        newSuggestionData.autoCompleteForMultiple.suggestions) ||
                    []
                }
                onSearch={search}
                onQueryChange={autoComplete}
                entityRegistry={entityRegistry}
                hideSearchBar={hideSearchBar}
            />
            <BodyBackground $isShowNavBarRedesign={isShowNavBarRedesign} />
            <Body>
                <Navigation $isShowNavBarRedesign={isShowNavBarRedesign}>
                    <FinalNavBar />
                </Navigation>
                <Content
                    $isShowNavBarRedesign={isShowNavBarRedesign}
                    $hideSearchBar={hideSearchBar}
                    $isNavBarCollapsed={isCollapsed}
                >
                    {children}
                </Content>
            </Body>
            <ProductUpdates />
        </>
    );
};

import { Skeleton } from 'antd';
import { debounce } from 'lodash';
import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import styled from 'styled-components/macro';

import { ANTD_GRAY_V2 } from '@app/entity/shared/constants';
import { ViewSelect } from '@app/entityV2/view/select/ViewSelect';
import { V2_SEARCH_BAR_VIEWS } from '@app/onboarding/configV2/HomePageOnboardingConfig';
import { SearchBarProps } from '@app/searchV2/SearchBar';
import useAppliedFilters from '@app/searchV2/filtersV2/context/useAppliedFilters';
import AutocompleteFooter from '@app/searchV2/searchBarV2/components/AutocompleteFooter';
import AutocompletePlaceholder from '@app/searchV2/searchBarV2/components/AutocompletePlaceholder';
import Filters from '@app/searchV2/searchBarV2/components/Filters';
import SearchBarInput from '@app/searchV2/searchBarV2/components/SearchBarInput';
import {
    AUTOCOMPLETE_DROPDOWN_ALIGN_WITH_NEW_NAV_BAR,
    DEBOUNCE_ON_SEARCH_TIMEOUT_MS,
} from '@app/searchV2/searchBarV2/constants';
import useSearchResultsOptions from '@app/searchV2/searchBarV2/hooks/useAutocompleteSuggestionsOptions';
import useFocusElementByCommandK from '@app/searchV2/searchBarV2/hooks/useFocusSearchBarByCommandK';
import useRecentlySearchedQueriesOptions from '@app/searchV2/searchBarV2/hooks/useRecentlySearchedQueriesOptions';
import useRecentlyViewedEntitiesOptions from '@app/searchV2/searchBarV2/hooks/useRecentlyViewedEntitiesOptions';
import useSelectOption from '@app/searchV2/searchBarV2/hooks/useSelectOption';
import useViewAllResultsOptions from '@app/searchV2/searchBarV2/hooks/useViewAllResultsOptions';
import { MIN_CHARACTER_COUNT_FOR_SEARCH } from '@app/searchV2/utils/constants';
import filterSearchQuery from '@app/searchV2/utils/filterSearchQuery';
import { useAppConfig, useIsShowSeparateSiblingsEnabled } from '@app/useAppConfig';
import { colors, radius, spacing, transition } from '@src/alchemy-components';
import { AutoComplete } from '@src/alchemy-components/components/AutoComplete';
import { SearchBarApi } from '@src/types.generated';

const BOX_SHADOW = `0px -3px 12px 0px rgba(236, 240, 248, 0.5) inset,
0px 3px 12px 0px rgba(255, 255, 255, 0.5) inset,
0px 20px 60px 0px rgba(0, 0, 0, 0.12)`;

const StyledAutoComplete = styled(AutoComplete)<{ $isShowNavBarRedesign?: boolean }>`
    width: 100%;
    max-width: ${(props) => (props.$isShowNavBarRedesign ? '632px' : '540px')};
`;

const AutoCompleteContainer = styled.div<{ viewsEnabled?: boolean; $isShowNavBarRedesign?: boolean }>`
    padding: 0 30px;
    align-items: center;
    border: ${(props) => (props.$isShowNavBarRedesign ? `2px solid ${colors.gray[100]}` : '2px solid transparent')};
    ${(props) => props.$isShowNavBarRedesign && 'box-shadow: 0px 1px 2px 0px rgba(33, 23, 95, 0.07)'};

    transition: border-color 0.3s ease;

    ${(props) =>
        props.viewsEnabled &&
        `
        border-radius: 8px;
        &:focus-within {
            border-color: ${props.$isShowNavBarRedesign ? colors.violet[300] : props.theme.styles['primary-color']};
        }
    `}
`;

export const Wrapper = styled.div<{ $open?: boolean; $isShowNavBarRedesign?: boolean }>`
    background: transparent;

    ${(props) =>
        props.$isShowNavBarRedesign &&
        `
        padding: ${radius.md};
        transition: all ${transition.easing['ease-in']} ${transition.duration.slow};
        border-radius: ${radius.lg} ${radius.lg} ${radius.none} ${radius.none};
    `}

    ${(props) =>
        props.$open &&
        props.$isShowNavBarRedesign &&
        `
        background: ${colors.gray[1500]};
        box-shadow: ${BOX_SHADOW};
    `}
`;

const ViewSelectContainer = styled.div`
    color: #fff;
    line-height: 20px;
    padding-right: 5.6px;

    &&& {
        border-left: 0px solid ${ANTD_GRAY_V2[5]};
    }
`;

const DropdownContainer = styled.div`
    overflow: auto;
    box-shadow: ${BOX_SHADOW};
    border-radius: ${radius.lg};
    background: ${colors.white};
`;

/**
 * Represents the search bar appearing in the default header view.
 */
export const SearchBarV2 = ({
    id,
    isLoading,
    initialQuery,
    placeholderText,
    onSearch,
    onQueryChange,
    onFilter,
    style,
    inputStyle,
    autoCompleteStyle,
    fixAutoComplete,
    showCommandK = false,
    viewsEnabled = false,
    combineSiblings = false,
    onFocus,
    onBlur,
    showViewAllResults = false,
    textColor,
    placeholderColor,
    isShowNavBarRedesign,
    searchResponse,
}: SearchBarProps) => {
    const appConfig = useAppConfig();
    const showAutoCompleteResults = appConfig?.config?.featureFlags?.showAutoCompleteResults;
    const isShowSeparateSiblingsEnabled = useIsShowSeparateSiblingsEnabled();
    const finalCombineSiblings = isShowSeparateSiblingsEnabled ? false : combineSiblings;

    const [searchQuery, setSearchQuery] = useState<string>(initialQuery || '');

    const entities = searchResponse?.entities;
    const facets = searchResponse?.facets;
    const isDataLoading = searchResponse?.loading;
    const searchAPIVariant = searchResponse?.searchAPIVariant;

    const [isDropdownVisible, setIsDropdownVisible] = useState(false);
    // used to show Loader when we searching for suggestions in both cases for the first time and after clearing searchQuery
    const [isDataInitialized, setIsDataInitialized] = useState<boolean>(false);
    const { appliedFilters, hasAppliedFilters, flatAppliedFilters, clear, updateFieldFilters } = useAppliedFilters();

    const searchInputRef = useRef(null);
    useFocusElementByCommandK(searchInputRef, !showCommandK);

    useEffect(() => onFilter?.(appliedFilters), [appliedFilters, onFilter]);

    useEffect(() => {
        if (searchQuery === '') setIsDataInitialized(false);
    }, [searchQuery]);

    useEffect(() => {
        if (!isDataLoading) setIsDataInitialized(true);
    }, [isDataLoading]);

    const recentlySearchedQueriesOptions = useRecentlySearchedQueriesOptions();
    const recentlyViewedEntitiesOptions = useRecentlyViewedEntitiesOptions();

    const initialOptions = useMemo(() => {
        return [...recentlyViewedEntitiesOptions, ...recentlySearchedQueriesOptions];
    }, [recentlyViewedEntitiesOptions, recentlySearchedQueriesOptions]);

    const viewAllResultsOptions = useViewAllResultsOptions(searchQuery, showViewAllResults);

    const isSearching = useMemo(() => {
        const minimalLengthOfQuery = searchAPIVariant === SearchBarApi.SearchAcrossEntities ? 3 : 1;

        const hasSearchQuery = searchQuery.length >= minimalLengthOfQuery;
        const hasAnyAppliedFilters = flatAppliedFilters.length > 0;

        return hasSearchQuery || hasAnyAppliedFilters;
    }, [searchQuery, flatAppliedFilters, searchAPIVariant]);

    const hasResults = useMemo(() => (entities?.length ?? 0) > 0, [entities?.length]);

    const searchResultsOptions = useSearchResultsOptions(
        entities,
        searchQuery,
        isDataLoading,
        isDataInitialized,
        finalCombineSiblings,
    );

    const options = useMemo(() => {
        if (!isSearching) return initialOptions;

        if (showAutoCompleteResults) {
            if (!isDataLoading && !hasResults) return [];
            return [...viewAllResultsOptions, ...searchResultsOptions];
        }

        return [];
    }, [
        isSearching,
        hasResults,
        initialOptions,
        searchResultsOptions,
        viewAllResultsOptions,
        showAutoCompleteResults,
        isDataLoading,
    ]);

    const searchBarWrapperRef = useRef<HTMLDivElement>(null);

    const onChangeHandler = useCallback(
        (value: string) => {
            const filteredQuery = filterSearchQuery(value);
            setSearchQuery(filteredQuery);
            if (value === '') clear();
        },
        [clear],
    );

    const onClearHandler = useCallback(() => {
        setSearchQuery('');
        clear();
    }, [clear]);

    // clear filters when this search bar is unmounted (ie. going from search results to home page)
    useEffect(() => () => onClearHandler(), [onClearHandler]);

    const onSearchHandler = showAutoCompleteResults
        ? debounce((query: string) => onQueryChange?.(query), DEBOUNCE_ON_SEARCH_TIMEOUT_MS)
        : undefined;

    const runSearching = useCallback(() => {
        const filteredSearchQuery = filterSearchQuery(searchQuery || '');
        let cleanedQuery = filteredSearchQuery.trim();
        if (cleanedQuery.length === 0) {
            cleanedQuery = '*';
        } else if (!cleanedQuery.includes('*') && cleanedQuery.length < MIN_CHARACTER_COUNT_FOR_SEARCH) {
            cleanedQuery = `${cleanedQuery}*`;
        }

        onSearch(filteredSearchQuery, flatAppliedFilters);
        setIsDropdownVisible(false);
    }, [searchQuery, flatAppliedFilters, onSearch]);

    const selectOption = useSelectOption(onSearch, onClearHandler, flatAppliedFilters);

    const viewsEnabledStyle = {
        ...style,
        backgroundColor: inputStyle?.backgroundColor,
    };

    const onDropdownVisibilityChange = useCallback((isOpen) => {
        if (!isOpen) {
            setIsDropdownVisible(isOpen);
        } else {
            // set timeout so that we allow search bar to grow in width and therefore allow autocomplete to grow
            setTimeout(() => {
                setIsDropdownVisible(isOpen);
            }, 0);
        }
    }, []);

    const onClearFilters = useCallback(() => clear(), [clear]);

    if (isLoading) return <Skeleton />;

    return (
        <Wrapper $open={isDropdownVisible} $isShowNavBarRedesign={isShowNavBarRedesign}>
            <AutoCompleteContainer
                viewsEnabled={viewsEnabled}
                $isShowNavBarRedesign={isShowNavBarRedesign}
                id={id}
                style={viewsEnabled ? viewsEnabledStyle : style}
                ref={searchBarWrapperRef}
            >
                <StyledAutoComplete
                    dataTestId="search-bar"
                    $isShowNavBarRedesign={isShowNavBarRedesign}
                    defaultActiveFirstOption={false}
                    style={autoCompleteStyle}
                    options={options}
                    filterOption={false}
                    dropdownRender={(props) => {
                        return (
                            <DropdownContainer>
                                {isSearching && (
                                    <Filters
                                        query={searchQuery ?? ''}
                                        appliedFilters={appliedFilters}
                                        updateFieldAppliedFilters={updateFieldFilters}
                                        facets={facets}
                                    />
                                )}
                                {props}
                                <AutocompleteFooter isSomethingSelected={!!searchQuery} />
                            </DropdownContainer>
                        );
                    }}
                    notFoundContent={
                        <AutocompletePlaceholder
                            hasAppliedFilters={hasAppliedFilters}
                            isSearching={isSearching}
                            onClearFilters={onClearFilters}
                        />
                    }
                    onSelect={selectOption}
                    onSearch={onSearchHandler}
                    defaultValue={initialQuery || undefined}
                    value={searchQuery}
                    onChange={onChangeHandler}
                    dropdownAlign={isShowNavBarRedesign ? AUTOCOMPLETE_DROPDOWN_ALIGN_WITH_NEW_NAV_BAR : undefined}
                    onClear={onClearHandler}
                    dropdownStyle={{
                        maxHeight: 1000,
                        overflowY: 'visible',
                        position: (fixAutoComplete && 'fixed') || 'relative',
                        backgroundColor: colors.gray[1500],
                        boxShadow: BOX_SHADOW,
                        ...(isShowNavBarRedesign
                            ? {
                                  padding: spacing.xsm,
                                  borderRadius: `${radius.none} ${radius.none} ${radius.lg} ${radius.lg}`,
                              }
                            : {}),
                    }}
                    onDropdownVisibleChange={onDropdownVisibilityChange}
                    open={isDropdownVisible}
                    dropdownContentHeight={480}
                    dropdownMatchSelectWidth={isShowNavBarRedesign ? 664 : true}
                >
                    <SearchBarInput
                        placeholder={placeholderText}
                        onSearch={runSearching}
                        style={inputStyle}
                        value={searchQuery}
                        onFocus={onFocus}
                        onBlur={onBlur}
                        viewsEnabled={viewsEnabled}
                        ref={searchInputRef}
                        textColor={textColor}
                        placeholderColor={placeholderColor}
                    />
                </StyledAutoComplete>
                {viewsEnabled && (
                    <ViewSelectContainer id={V2_SEARCH_BAR_VIEWS}>
                        <ViewSelect />
                    </ViewSelectContainer>
                )}
            </AutoCompleteContainer>
        </Wrapper>
    );
};

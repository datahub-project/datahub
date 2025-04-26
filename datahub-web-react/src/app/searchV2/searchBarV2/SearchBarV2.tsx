import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import styled from 'styled-components/macro';

import { SearchBarProps } from '@app/searchV2/SearchBar';
import useAppliedFilters from '@app/searchV2/filtersV2/context/useAppliedFilters';
import AutocompletePlaceholder from '@app/searchV2/searchBarV2/components/AutocompletePlaceholder';
import SearchBarDropdown from '@app/searchV2/searchBarV2/components/SearchBarDropdown';
import SearchBarInput from '@app/searchV2/searchBarV2/components/SearchBarInput';
import Skeleton from '@app/searchV2/searchBarV2/components/Skeleton';
import {
    AUTOCOMPLETE_DROPDOWN_ALIGN,
    AUTOCOMPLETE_DROPDOWN_ALIGN_WITH_NEW_NAV_BAR,
    BOX_SHADOW,
} from '@app/searchV2/searchBarV2/constants';
import useFiltersMapFromQueryParams from '@app/searchV2/searchBarV2/hooks/useFiltersMapFromQueryParams';
import useFocusElementByCommandK from '@app/searchV2/searchBarV2/hooks/useFocusSearchBarByCommandK';
import useOptions from '@app/searchV2/searchBarV2/hooks/useOptions';
import { useSearchBarData } from '@app/searchV2/searchBarV2/hooks/useSearchBarData';
import useSelectOption from '@app/searchV2/searchBarV2/hooks/useSelectOption';
import useSelectedView from '@app/searchV2/searchBarV2/hooks/useSelectedView';
import { MIN_CHARACTER_COUNT_FOR_SEARCH, SEARCH_BAR_CLASS_NAME } from '@app/searchV2/utils/constants';
import filterSearchQuery from '@app/searchV2/utils/filterSearchQuery';
import { useAppConfig, useIsShowSeparateSiblingsEnabled } from '@app/useAppConfig';
import { colors, radius, spacing } from '@src/alchemy-components';
import { AutoComplete } from '@src/alchemy-components/components/AutoComplete';
import { SearchBarApi } from '@src/types.generated';

const Wrapper = styled.div``;

/**
 * Represents the search bar appearing in the default header view.
 */
export const SearchBarV2 = ({
    id,
    isLoading,
    initialQuery,
    placeholderText,
    onSearch,
    fixAutoComplete,
    showCommandK = false,
    viewsEnabled = false,
    combineSiblings = false,
    onFocus,
    onBlur,
    showViewAllResults = false,
    isShowNavBarRedesign,
}: SearchBarProps) => {
    const appConfig = useAppConfig();
    const showAutoCompleteResults = appConfig?.config?.featureFlags?.showAutoCompleteResults;
    const isShowSeparateSiblingsEnabled = useIsShowSeparateSiblingsEnabled();
    const shouldCombineSiblings = isShowSeparateSiblingsEnabled ? false : combineSiblings;

    const [searchQuery, setSearchQuery] = useState<string>(initialQuery || '');
    const [isDropdownVisible, setIsDropdownVisible] = useState(false);
    const filtersFromQueryParams = useFiltersMapFromQueryParams();
    const { appliedFilters, hasAppliedFilters, flatAppliedFilters, clear, updateFieldFilters } =
        useAppliedFilters(filtersFromQueryParams);
    const { hasSelectedView, clearSelectedView } = useSelectedView();
    const {
        entitiesWithMatchedFields,
        facets,
        loading: isDataLoading,
        searchAPIVariant,
    } = useSearchBarData(searchQuery, appliedFilters);

    const searchInputRef = useRef(null);
    useFocusElementByCommandK(searchInputRef, !showCommandK);

    const isSearching = useMemo(() => {
        const minimalLengthOfQuery = searchAPIVariant === SearchBarApi.SearchAcrossEntities ? 3 : 1;

        const hasSearchQuery = searchQuery.length >= minimalLengthOfQuery;
        const hasAnyAppliedFilters = flatAppliedFilters.length > 0;

        return hasSearchQuery || hasAnyAppliedFilters;
    }, [searchQuery, flatAppliedFilters, searchAPIVariant]);

    const options = useOptions(
        searchQuery,
        showViewAllResults,
        entitiesWithMatchedFields,
        !!isDataLoading,
        shouldCombineSiblings,
        isSearching,
        showAutoCompleteResults,
    );

    const clearQueryAndFilters = useCallback(() => {
        setSearchQuery('');
        clear();
    }, [clear]);

    const selectOption = useSelectOption(onSearch, clearQueryAndFilters, flatAppliedFilters);

    // clear filters when this search bar is unmounted (ie. going from search results to home page)
    useEffect(() => () => clearQueryAndFilters(), [clearQueryAndFilters]);

    const onSearchHandler = useCallback(() => {
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

    const onSelectHandler = useCallback(
        (value, option) => {
            selectOption(value, option);
            setIsDropdownVisible(false);
        },
        [selectOption],
    );

    const onChangeHandler = useCallback(
        (value: string) => {
            const filteredQuery = filterSearchQuery(value);
            setSearchQuery(filteredQuery);
            if (value === '') clear();
        },
        [clear],
    );

    const onDropdownVisibilityChangeHandler = useCallback((isOpen) => setIsDropdownVisible(isOpen), []);

    const onClearFiltersAndSelectedViewHandler = useCallback(() => {
        clear();
        clearSelectedView();
    }, [clear, clearSelectedView]);

    const onClearHandler = useCallback(() => clearQueryAndFilters(), [clearQueryAndFilters]);

    if (isLoading) return <Skeleton />;

    return (
        <Wrapper id={id} className={SEARCH_BAR_CLASS_NAME}>
            <AutoComplete
                dataTestId="search-bar"
                defaultActiveFirstOption={false}
                options={options}
                filterOption={false}
                dropdownRender={(menu) => (
                    <SearchBarDropdown
                        menu={menu}
                        query={searchQuery}
                        filters={appliedFilters}
                        updateFilters={updateFieldFilters}
                        facets={facets}
                        isSearching={isSearching}
                    />
                )}
                notFoundContent={
                    <AutocompletePlaceholder
                        hasAppliedFilters={hasAppliedFilters}
                        hasSelectedView={hasSelectedView}
                        isSearching={isSearching}
                        onClearFilters={onClearFiltersAndSelectedViewHandler}
                    />
                }
                onSelect={onSelectHandler}
                defaultValue={initialQuery || undefined}
                value={searchQuery}
                onChange={onChangeHandler}
                dropdownAlign={
                    isShowNavBarRedesign ? AUTOCOMPLETE_DROPDOWN_ALIGN_WITH_NEW_NAV_BAR : AUTOCOMPLETE_DROPDOWN_ALIGN
                }
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
                onDropdownVisibleChange={onDropdownVisibilityChangeHandler}
                open={isDropdownVisible}
                dropdownContentHeight={480}
            >
                <SearchBarInput
                    placeholder={placeholderText}
                    onSearch={onSearchHandler}
                    value={searchQuery}
                    onFocus={onFocus}
                    onBlur={onBlur}
                    ref={searchInputRef}
                    showCommandK={showCommandK}
                    isDropdownOpened={isDropdownVisible}
                    viewsEnabled={viewsEnabled}
                />
            </AutoComplete>
        </Wrapper>
    );
};

import { colors, radius, spacing, transition } from '@src/alchemy-components';
import { AutoComplete } from '@src/alchemy-components/components/AutoComplete';
import { Skeleton } from 'antd';
import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import styled from 'styled-components/macro';

import { SearchBarProps } from '@app/searchV2/SearchBar';
import useAppliedFilters from '@app/searchV2/filtersV2/context/useAppliedFilters';
import AutocompleteDropdown from '@app/searchV2/searchBarV2/components/AutocompleteDropdown';
import AutocompletePlaceholder from '@app/searchV2/searchBarV2/components/AutocompletePlaceholder';
import SearchBarInput from '@app/searchV2/searchBarV2/components/SearchBarInput';
import {
    AUTOCOMPLETE_DROPDOWN_ALIGN_WITH_NEW_NAV_BAR,
    BOX_SHADOW
} from '@app/searchV2/searchBarV2/constants';
import useFocusElementByCommandK from '@app/searchV2/searchBarV2/hooks/useFocusSearchBarByCommandK';
import useOptions from '@app/searchV2/searchBarV2/hooks/useOptions';
import useSelectOption from '@app/searchV2/searchBarV2/hooks/useSelectOption';
import { useSearchBarData } from '@app/searchV2/useSearchBarData';
import { MIN_CHARACTER_COUNT_FOR_SEARCH } from '@app/searchV2/utils/constants';
import filterSearchQuery from '@app/searchV2/utils/filterSearchQuery';
import { useAppConfig, useIsShowSeparateSiblingsEnabled } from '@app/useAppConfig';
import { SearchBarApi } from '@src/types.generated';
import { AUTOCOMPLETE_DROPDOWN_ALIGN } from './constants';

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
    const { appliedFilters, hasAppliedFilters, flatAppliedFilters, clear, updateFieldFilters } = useAppliedFilters();
    const {
        entities,
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
        entities,
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

    const onClearFiltersHandler = useCallback(() => clear(), [clear]);

    const onClearHandler = useCallback(() => clearQueryAndFilters(), [clearQueryAndFilters]);

    if (isLoading) return <Skeleton />;

    return (
        <AutoComplete
            id={id}
            dataTestId="search-bar"
            defaultActiveFirstOption={false}
            options={options}
            filterOption={false}
            dropdownRender={(menu) => (
                <AutocompleteDropdown
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
                    isSearching={isSearching}
                    onClearFilters={onClearFiltersHandler}
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
    );
};

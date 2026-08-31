import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { useDebounce } from 'react-use';

import { ItemType } from '@components/components/Menu/types';
import { sortMenuItems } from '@components/components/Menu/utils';

import useStructuredProperties from '@app/entityV2/summary/properties/hooks/useStructuredProperties';
import MenuLoader from '@app/entityV2/summary/properties/menuAddProperty/components/MenuLoader';
import MenuNoResultsFound from '@app/entityV2/summary/properties/menuAddProperty/components/MenuNoResultsFound';
import MenuSearchBar from '@app/entityV2/summary/properties/menuAddProperty/components/MenuSearchBar';
import { AssetProperty } from '@app/entityV2/summary/properties/types';
import { assetPropertyToMenuItem } from '@app/entityV2/summary/properties/utils';
import { usePageTemplateContext } from '@app/homeV3/context/PageTemplateContext';
import { DEBOUNCE_SEARCH_MS } from '@app/shared/constants';

export default function useStructuredPropertiesMenuItems(onClick: (property: AssetProperty) => void) {
    const { summaryElements } = usePageTemplateContext();
    const [query, setQuery] = useState<string>('');
    const [debouncedQuery, setDebouncedQuery] = useState<string>('');
    // The flag used to return empty menu if an asset have no any structured properties
    const [hasAnyStructuredProperties, setHasAnyStructuredProperties] = useState<boolean>(false);

    const { structuredProperties, loading } = useStructuredProperties(debouncedQuery);

    useDebounce(() => setDebouncedQuery(query), DEBOUNCE_SEARCH_MS, [query]);

    const onMenuItemClick = useCallback(
        (property: AssetProperty) => {
            setQuery(''); // reset query on click
            onClick(property);
        },
        [onClick],
    );

    const visibleStructuredPropertyUrns = useMemo(
        () =>
            new Set(
                summaryElements
                    ?.filter((el) => el.structuredProperty)
                    .map((el) => el.structuredProperty?.urn)
                    .filter((urn): urn is string => urn !== undefined) ?? [],
            ),
        [summaryElements],
    );

    const filteredProperties = useMemo(
        () =>
            structuredProperties.filter((assetProperty) => {
                if (assetProperty.structuredProperty) {
                    return !visibleStructuredPropertyUrns.has(assetProperty.structuredProperty.urn);
                }
                return true;
            }),
        [structuredProperties, visibleStructuredPropertyUrns],
    );

    // Reset `hasAnyStructuredProperties` and `query` to recompute `hasAnyStructuredProperties`
    // after updating of structured properties
    useEffect(() => {
        setHasAnyStructuredProperties(false);
        setQuery('');
    }, [visibleStructuredPropertyUrns]);

    useEffect(() => {
        if (!hasAnyStructuredProperties && filteredProperties.length > 0) {
            setHasAnyStructuredProperties(true);
        }
    }, [filteredProperties, hasAnyStructuredProperties]);

    const noResultsFoundItem: ItemType = useMemo(
        () => ({
            type: 'item',
            key: 'noResults',
            title: 'noResults',
            disabled: true,
            render: () => <MenuNoResultsFound />,
        }),
        [],
    );

    const shouldShowNoResultsFound = useMemo(
        () => !!query && filteredProperties.length === 0,
        [query, filteredProperties],
    );

    const searchBarItem: ItemType = useMemo(
        () => ({
            type: 'item',
            key: 'search',
            title: 'Search',
            render: () => (
                <MenuSearchBar
                    value={query}
                    onChange={(value) => setQuery(value)}
                    dataTestId="structured-property-search"
                />
            ),
        }),
        [query],
    );

    const loaderItem: ItemType = useMemo(
        () => ({
            type: 'item',
            key: 'loading',
            title: 'Loading',
            disabled: true,
            render: () => <MenuLoader />,
        }),
        [],
    );

    const structuredPropertyMenuItems = useMemo(
        () =>
            sortMenuItems(
                filteredProperties.map((assetProperty) => assetPropertyToMenuItem(assetProperty, onMenuItemClick)),
            ),
        [filteredProperties, onMenuItemClick],
    );

    const shouldShowLoading = useMemo(
        () => loading && structuredProperties.length === 0,
        [loading, structuredProperties.length],
    );

    const menuItems: ItemType[] = useMemo(() => {
        if (!hasAnyStructuredProperties) return [];
        const items: ItemType[] = [searchBarItem];

        if (shouldShowLoading) {
            items.push(loaderItem);
        }

        if (!shouldShowLoading && shouldShowNoResultsFound) {
            items.push(noResultsFoundItem);
        }

        items.push(...structuredPropertyMenuItems);

        return items;
    }, [
        hasAnyStructuredProperties,
        searchBarItem,
        shouldShowNoResultsFound,
        noResultsFoundItem,
        shouldShowLoading,
        loaderItem,
        structuredPropertyMenuItems,
    ]);

    return menuItems;
}

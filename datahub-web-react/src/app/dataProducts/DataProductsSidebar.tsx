import { EmptyState, Tooltip } from '@components';
import { Plus } from '@phosphor-icons/react/dist/csr/Plus';
import { Storefront } from '@phosphor-icons/react/dist/csr/Storefront';
import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { matchPath, useLocation } from 'react-router-dom';
import { useDebounce } from 'react-use';
import styled from 'styled-components';

import { useUserContext } from '@app/context/useUserContext';
import { DataProductChildRow } from '@app/dataProducts/DataProductChildRow';
import DataProductsSearch from '@app/dataProducts/DataProductsSearch';
import DataProductsSidebarSearchFilters from '@app/dataProducts/DataProductsSidebarSearchFilters';
import DataProductsSidebarSearchResults from '@app/dataProducts/DataProductsSidebarSearchResults';
import DataProductsSidebarSecondaryFilters from '@app/dataProducts/DataProductsSidebarSecondaryFilters';
import { useDataProductsEntityContext } from '@app/dataProducts/context/DataProductsEntityContext';
import useDataProductsSidebarFacetOptions from '@app/dataProducts/hooks/useDataProductsSidebarFacetOptions';
import useDataProductsSidebarSearch from '@app/dataProducts/hooks/useDataProductsSidebarSearch';
import { DataProductEntity } from '@app/dataProducts/dataProductsTypes';
import useDataProductRoots from '@app/dataProducts/useDataProductRoots';
import { isRootDataProduct, mergeDataProductEntities } from '@app/dataProducts/utils/dataProductsDataProductEntity';
import {
    mergeDataProductsVisibleRootProducts,
    resolveDataProductsFallbackRootUrn,
} from '@app/dataProducts/utils/dataProductsSidebarBrowse';
import {
    SECONDARY_BROWSE_FILTERS,
    SecondaryBrowseFilter,
    buildDataProductsSearchModeState,
    isSecondaryBrowseFilter,
    nextPromotedBrowseFilters,
} from '@app/dataProducts/utils/dataProductsSidebarMode';
import HierarchicalBrowseSidebar from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/HierarchicalBrowseSidebar';
import { SidebarCreateButton } from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/HierarchicalBrowseSidebar.components';
import SidebarAddFilter from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/SidebarAddFilter';
import SidebarHomeNavLink from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/SidebarHomeNavLink';
import { TreeSectionHeader } from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/TreeSectionHeader';
import { PageRoutes } from '@conf/Global';

import { useScrollDataProductsQuery } from '@graphql/dataProductsBrowse.generated';
import { EntityType, FilterOperator } from '@types';

const EmptyStateWrapper = styled.div`
    flex: 1;
    display: flex;
    align-items: center;
    justify-content: center;
    padding: 24px 12px;
`;

type Props = {
    isCollapsed: boolean;
    onToggleCollapsed: () => void;
    onExpandSidebar: () => void;
};

export default function DataProductsSidebar({ isCollapsed, onToggleCollapsed, onExpandSidebar }: Props) {
    const { t } = useTranslation('misc');
    const location = useLocation();
    const [searchInput, setSearchInput] = useState('');
    const [debouncedQuery, setDebouncedQuery] = useState('');
    const [isProductsExpanded, setIsProductsExpanded] = useState(true);
    const [selectedDomainUrns, setSelectedDomainUrns] = useState<string[]>([]);
    const [selectedTagUrns, setSelectedTagUrns] = useState<string[]>([]);
    const [selectedTermUrns, setSelectedTermUrns] = useState<string[]>([]);
    const [selectedOwnerUrns, setSelectedOwnerUrns] = useState<string[]>([]);
    const [selectedApplicationUrns, setSelectedApplicationUrns] = useState<string[]>([]);
    const [promotedBrowseFilters, setPromotedBrowseFilters] = useState<Set<SecondaryBrowseFilter>>(new Set());
    const [filterToAutoOpen, setFilterToAutoOpen] = useState<SecondaryBrowseFilter | null>(null);
    const [autoOpenNonce, setAutoOpenNonce] = useState(0);

    const userContext = useUserContext();
    const viewUrn = userContext.localState?.selectedViewUrn;

    const {
        expandedDataProductUrns,
        selectedUrn,
        toggleDataProduct,
        expandAllDataProducts,
        collapseAllExpanded,
        refetchKey,
        entityData,
        optimisticDataProducts,
        openCreateModal,
        syncOptimisticWithIndexed,
    } = useDataProductsEntityContext();

    const isHomeSelected = !!matchPath(location.pathname, { path: PageRoutes.DATA_PRODUCTS, exact: true });

    useDebounce(() => setDebouncedQuery(searchInput), 300, [searchInput]);

    const searchModeInput = useMemo(
        () => ({
            domainUrns: selectedDomainUrns,
            tagUrns: selectedTagUrns,
            termUrns: selectedTermUrns,
            ownerUrns: selectedOwnerUrns,
            applicationUrns: selectedApplicationUrns,
        }),
        [selectedDomainUrns, selectedTagUrns, selectedTermUrns, selectedOwnerUrns, selectedApplicationUrns],
    );

    const { isSearchActive, shouldFetchSearch } = useMemo(
        () =>
            buildDataProductsSearchModeState({
                searchInput,
                debouncedSearchInput: debouncedQuery,
                filters: searchModeInput,
            }),
        [searchInput, debouncedQuery, searchModeInput],
    );

    useEffect(() => {
        setPromotedBrowseFilters((prev) =>
            nextPromotedBrowseFilters(prev, {
                termUrns: selectedTermUrns,
                applicationUrns: selectedApplicationUrns,
            }),
        );
    }, [selectedTermUrns, selectedApplicationUrns]);

    const demoteBrowseFilter = useCallback((key: SecondaryBrowseFilter) => {
        setPromotedBrowseFilters((prev) => {
            if (!prev.has(key)) return prev;
            const next = new Set(prev);
            next.delete(key);
            return next;
        });
    }, []);

    const promoteBrowseFilter = useCallback((key: SecondaryBrowseFilter) => {
        setPromotedBrowseFilters((prev) => {
            if (prev.has(key)) return prev;
            const next = new Set(prev);
            next.add(key);
            return next;
        });
    }, []);

    const handleAddBrowseFilter = useCallback(
        (value: string) => {
            if (!isSecondaryBrowseFilter(value)) return;
            promoteBrowseFilter(value);
            setFilterToAutoOpen(value);
            setAutoOpenNonce((n) => n + 1);
        },
        [promoteBrowseFilter],
    );

    const handleTermsChange = useCallback(
        (urns: string[]) => {
            setSelectedTermUrns(urns);
            setFilterToAutoOpen(null);
            if (urns.length === 0) demoteBrowseFilter('term');
        },
        [demoteBrowseFilter],
    );

    const handleApplicationsChange = useCallback(
        (urns: string[]) => {
            setSelectedApplicationUrns(urns);
            setFilterToAutoOpen(null);
            if (urns.length === 0) demoteBrowseFilter('application');
        },
        [demoteBrowseFilter],
    );

    const handleClearSearch = useCallback(() => {
        setSearchInput('');
        setDebouncedQuery('');
        setSelectedDomainUrns([]);
        setSelectedTagUrns([]);
        setSelectedTermUrns([]);
        setSelectedOwnerUrns([]);
        setSelectedApplicationUrns([]);
        setPromotedBrowseFilters(new Set());
        setFilterToAutoOpen(null);
    }, []);

    const { domainOptions, tagOptions, termOptions, ownerOptions, applicationOptions } =
        useDataProductsSidebarFacetOptions({
            searchQuery: debouncedQuery,
            domainUrns: selectedDomainUrns,
            tagUrns: selectedTagUrns,
            termUrns: selectedTermUrns,
            ownerUrns: selectedOwnerUrns,
            applicationUrns: selectedApplicationUrns,
            viewUrn,
            includeTermFacets: promotedBrowseFilters.has('term') || selectedTermUrns.length > 0,
            includeApplicationFacets: promotedBrowseFilters.has('application') || selectedApplicationUrns.length > 0,
        });

    const {
        dataProducts: searchResults,
        total: searchTotal,
        loading: searchLoading,
        isRefreshing: searchRefreshing,
        scrollRef: searchScrollRef,
    } = useDataProductsSidebarSearch({
        searchQuery: debouncedQuery,
        ...searchModeInput,
        viewUrn,
        skip: !shouldFetchSearch,
    });

    const searchResultsLoading =
        (isSearchActive && !shouldFetchSearch) || (searchLoading && searchResults.length === 0);

    const {
        data: rootProducts,
        scrollRef: rootScrollRef,
        refetch: refetchProducts,
    } = useDataProductRoots(isSearchActive);

    const optimisticRootProducts = useMemo(
        () => optimisticDataProducts.filter(isRootDataProduct),
        [optimisticDataProducts],
    );

    const mergedRootProducts = useMemo(
        () => mergeDataProductEntities(rootProducts, optimisticRootProducts),
        [optimisticRootProducts, rootProducts],
    );

    const addFilterOptions = useMemo(() => {
        const labels: Record<SecondaryBrowseFilter, string> = {
            term: t('context.termFilter.label'),
            application: t('dataProducts.filterApplication'),
        };
        return SECONDARY_BROWSE_FILTERS.filter((key) => !promotedBrowseFilters.has(key)).map((key) => ({
            value: key,
            label: labels[key],
        }));
    }, [promotedBrowseFilters, t]);

    const fallbackRootUrn = useMemo(
        () => resolveDataProductsFallbackRootUrn(entityData, mergedRootProducts, isSearchActive),
        [entityData, mergedRootProducts, isSearchActive],
    );

    const { data: fallbackData } = useScrollDataProductsQuery({
        skip: !fallbackRootUrn,
        variables: {
            input: {
                query: '*',
                types: [EntityType.DataProduct],
                count: 1,
                orFilters: [
                    {
                        and: [
                            {
                                field: 'urn',
                                condition: FilterOperator.Equal,
                                values: [fallbackRootUrn ?? ''],
                            },
                        ],
                    },
                ],
            },
        },
    });

    const visibleProducts: DataProductEntity[] = useMemo(() => {
        if (!fallbackRootUrn) return mergedRootProducts;
        const fallbackProducts = (fallbackData?.scrollAcrossEntities?.searchResults ?? [])
            .map((r) => r.entity)
            .filter((e): e is DataProductEntity => e?.__typename === 'DataProduct');
        return mergeDataProductsVisibleRootProducts(mergedRootProducts, fallbackProducts);
    }, [mergedRootProducts, fallbackData, fallbackRootUrn]);

    // Only pass fetched/indexed URNs — visibleProducts includes optimistic rows and would prune them early.
    useEffect(() => {
        const fallbackIndexedUrns = (fallbackData?.scrollAcrossEntities?.searchResults ?? [])
            .map((r) => r.entity?.urn)
            .filter((urn): urn is string => !!urn);
        syncOptimisticWithIndexed([
            ...searchResults.map((product) => product.urn),
            ...rootProducts.map((product) => product.urn),
            ...fallbackIndexedUrns,
        ]);
    }, [searchResults, rootProducts, fallbackData, syncOptimisticWithIndexed]);

    useEffect(() => {
        if (refetchKey > 0 && !isSearchActive) {
            refetchProducts();
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [refetchKey, isSearchActive]);

    const isSectionExpanded = expandedDataProductUrns.size > 0;
    const handleToggleExpandAll = useCallback(() => {
        if (isSectionExpanded) {
            collapseAllExpanded();
            return;
        }
        setIsProductsExpanded(true);
        const expandable = visibleProducts.filter((p) => (p.childDataProducts?.total ?? 0) > 0).map((p) => p.urn);
        expandAllDataProducts(expandable);
    }, [isSectionExpanded, visibleProducts, collapseAllExpanded, expandAllDataProducts]);

    const headerActions = (
        <Tooltip title={t('dataProducts.createTooltip')} placement="bottom" showArrow={false}>
            <SidebarCreateButton
                variant="filled"
                color="primary"
                isCircle
                icon={{ icon: Plus }}
                onClick={openCreateModal}
                data-testid="create-data-products-data-product-button"
            />
        </Tooltip>
    );

    const hasVisibleFilters =
        domainOptions.length > 0 ||
        selectedDomainUrns.length > 0 ||
        ownerOptions.length > 0 ||
        selectedOwnerUrns.length > 0 ||
        tagOptions.length > 0 ||
        selectedTagUrns.length > 0 ||
        promotedBrowseFilters.size > 0 ||
        addFilterOptions.length > 0;

    const filters = hasVisibleFilters ? (
        <>
            <DataProductsSidebarSearchFilters
                selectedDomainUrns={selectedDomainUrns}
                selectedOwnerUrns={selectedOwnerUrns}
                selectedTagUrns={selectedTagUrns}
                domainOptions={domainOptions}
                ownerOptions={ownerOptions}
                tagOptions={tagOptions}
                onDomainsChange={setSelectedDomainUrns}
                onOwnersChange={setSelectedOwnerUrns}
                onTagsChange={setSelectedTagUrns}
            />
            <DataProductsSidebarSecondaryFilters
                promotedBrowseFilters={promotedBrowseFilters}
                filterToAutoOpen={filterToAutoOpen}
                autoOpenNonce={autoOpenNonce}
                selectedTermUrns={selectedTermUrns}
                selectedApplicationUrns={selectedApplicationUrns}
                termOptions={termOptions}
                applicationOptions={applicationOptions}
                onTermsChange={handleTermsChange}
                onApplicationsChange={handleApplicationsChange}
            />
            <SidebarAddFilter
                options={addFilterOptions}
                onAdd={handleAddBrowseFilter}
                dataTestId="data-products-sidebar-add-filter"
            />
        </>
    ) : null;

    return (
        <HierarchicalBrowseSidebar
            title={t('dataProducts.sidebarTitle')}
            isCollapsed={isCollapsed}
            onToggleCollapsed={onToggleCollapsed}
            onExpandSidebar={onExpandSidebar}
            headerActions={headerActions}
            dataTestId="data-products-sidebar"
            collapseButtonTestId="data-products-sidebar-collapse-button"
            collapsedSearchAriaLabel={t('dataProducts.searchAriaLabel')}
            collapsedSearchTestId="data-products-sidebar-search-icon"
            search={<DataProductsSearch value={searchInput} onChange={setSearchInput} />}
            filters={filters}
            homeNav={
                <SidebarHomeNavLink
                    to={PageRoutes.DATA_PRODUCTS}
                    isSelected={isHomeSelected}
                    label={t('dataProducts.home')}
                    data-testid="data-products-sidebar-home"
                />
            }
        >
            {isSearchActive ? (
                <DataProductsSidebarSearchResults
                    dataProducts={searchResults}
                    total={searchTotal}
                    loading={searchResultsLoading}
                    isRefreshing={searchRefreshing}
                    selectedUrn={selectedUrn}
                    scrollRef={searchScrollRef}
                    onClear={handleClearSearch}
                />
            ) : (
                <div data-testid="data-products-sidebar-tree">
                    <TreeSectionHeader
                        level={0}
                        label={t('dataProducts.dataProductsSection')}
                        isExpanded={isProductsExpanded}
                        onToggle={() => setIsProductsExpanded((v) => !v)}
                        onToggleExpandAll={handleToggleExpandAll}
                        isAllExpanded={isSectionExpanded}
                        expandAllLabel={t('dataProducts.expandAll')}
                        collapseAllLabel={t('dataProducts.collapseAll')}
                        testId="data-products-sidebar-products-section"
                    />
                    {isProductsExpanded && (
                        <>
                            {visibleProducts.length === 0 && (
                                <EmptyStateWrapper>
                                    <EmptyState
                                        icon={Storefront}
                                        title={t('dataProducts.emptyTreeTitle')}
                                        description={t('dataProducts.emptyTreeDescription')}
                                        size="sm"
                                    />
                                </EmptyStateWrapper>
                            )}
                            {visibleProducts.map((product) => (
                                <DataProductChildRow
                                    key={product.urn}
                                    level={0}
                                    dataProduct={product}
                                    isExpanded={expandedDataProductUrns.has(product.urn)}
                                    isSelected={selectedUrn === product.urn}
                                    expandedDataProductUrns={expandedDataProductUrns}
                                    selectedUrn={selectedUrn}
                                    onToggle={() => toggleDataProduct(product.urn)}
                                    onToggleDataProduct={toggleDataProduct}
                                />
                            ))}
                            <div ref={rootScrollRef} style={{ height: 1 }} />
                        </>
                    )}
                </div>
            )}
        </HierarchicalBrowseSidebar>
    );
}

import { EmptyState } from '@components';
import { Storefront } from '@phosphor-icons/react/dist/csr/Storefront';
import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { matchPath, useLocation } from 'react-router-dom';
import styled from 'styled-components';

import { SimpleSelect } from '@components/components/Select/SimpleSelect';

import { DataProductChildRow } from '@app/marketplace/DataProductChildRow';
import MarketplaceSearch from '@app/marketplace/MarketplaceSearch';
import { useMarketplaceEntityContext } from '@app/marketplace/context/MarketplaceEntityContext';
import { DataProductEntity } from '@app/marketplace/marketplaceTypes';
import useDataProductRoots from '@app/marketplace/useDataProductRoots';
import HierarchicalBrowseSidebar from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/HierarchicalBrowseSidebar';
import SidebarHomeNavLink from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/SidebarHomeNavLink';
import { TreeSectionHeader } from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/TreeSectionHeader';
import { PageRoutes } from '@conf/Global';

import { useScrollDataProductsQuery } from '@graphql/marketplaceBrowse.generated';
import { EntityType } from '@types';

const EmptyStateWrapper = styled.div`
    flex: 1;
    display: flex;
    align-items: center;
    justify-content: center;
    padding: 24px 12px;
`;

const ALL_OPTION = '__all__';

type Props = {
    isCollapsed: boolean;
    onToggleCollapsed: () => void;
    onExpandSidebar: () => void;
};

function productHasApplication(product: DataProductEntity, applicationUrn: string): boolean {
    return (product.applications ?? []).some((assoc) => assoc.application?.urn === applicationUrn);
}

export default function MarketplaceSidebar({ isCollapsed, onToggleCollapsed, onExpandSidebar }: Props) {
    const { t } = useTranslation('misc');
    const location = useLocation();
    const [sidebarFilter, setSidebarFilter] = useState(ALL_OPTION);
    const [isProductsExpanded, setIsProductsExpanded] = useState(true);

    const {
        expandedDataProductUrns,
        selectedUrn,
        toggleDataProduct,
        expandAllDataProducts,
        collapseAllExpanded,
        refetchKey,
        entityData,
    } = useMarketplaceEntityContext();

    const isHomeSelected = !!matchPath(location.pathname, { path: PageRoutes.MARKETPLACE, exact: true });

    const { data: rootProducts, scrollRef: rootScrollRef, refetch: refetchProducts } = useDataProductRoots();

    // Prefer the outermost ancestor when viewing a nested product; otherwise the product itself
    // if it isn't already among the loaded roots.
    const fallbackRootUrn = useMemo(() => {
        if (entityData?.entityType !== EntityType.DataProduct) return null;
        const ancestors = entityData.parentDataProducts ?? [];
        if (ancestors.length > 0) {
            const rootAncestorUrn = ancestors[ancestors.length - 1]?.urn;
            if (rootAncestorUrn && !rootProducts.some((p) => p.urn === rootAncestorUrn)) {
                return rootAncestorUrn;
            }
            return null;
        }
        if (!rootProducts.some((p) => p.urn === entityData.urn)) {
            return entityData.urn;
        }
        return null;
    }, [entityData, rootProducts]);

    const { data: fallbackData } = useScrollDataProductsQuery({
        skip: !fallbackRootUrn,
        variables: {
            input: {
                query: '*',
                types: [EntityType.DataProduct],
                count: 1,
                orFilters: [{ and: [{ field: 'urn', condition: 'EQUAL' as any, values: [fallbackRootUrn ?? ''] }] }],
            },
        },
    });

    const allProducts: DataProductEntity[] = useMemo(() => {
        if (!fallbackRootUrn) return rootProducts;
        const fallbackProducts = (fallbackData?.scrollAcrossEntities?.searchResults ?? [])
            .map((r) => r.entity)
            .filter((e): e is DataProductEntity => e?.__typename === 'DataProduct');
        if (fallbackProducts.length === 0) return rootProducts;
        const existingUrns = new Set(rootProducts.map((p) => p.urn));
        const newProducts = fallbackProducts.filter((p) => !existingUrns.has(p.urn));
        return newProducts.length > 0 ? [...newProducts, ...rootProducts] : rootProducts;
    }, [rootProducts, fallbackData, fallbackRootUrn]);

    useEffect(() => {
        if (refetchKey > 0) {
            refetchProducts();
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [refetchKey]);

    const applicationOptions = useMemo(() => {
        const seen = new Map<string, string>();
        allProducts.forEach((product) => {
            (product.applications ?? []).forEach((assoc) => {
                const urn = assoc.application?.urn;
                const label = assoc.application?.properties?.name;
                if (urn && label && !seen.has(urn)) seen.set(urn, label);
            });
        });
        return Array.from(seen.entries()).map(([urn, label]) => ({ value: urn, label }));
    }, [allProducts]);

    // Prefer Applications when products are linked to any; otherwise fall back to Domain so the
    // sidebar still has a useful filter (demo data often has domains but no applications).
    const useApplicationFilter = applicationOptions.length > 0;

    const domainOptions = useMemo(() => {
        if (useApplicationFilter) return [];
        const seen = new Map<string, string>();
        allProducts.forEach((product) => {
            const urn = product.domain?.domain?.urn;
            const label = product.domain?.domain?.properties?.name;
            if (urn && label && !seen.has(urn)) seen.set(urn, label);
        });
        return Array.from(seen.entries()).map(([urn, label]) => ({ value: urn, label }));
    }, [allProducts, useApplicationFilter]);

    const filterOptions = useApplicationFilter ? applicationOptions : domainOptions;

    useEffect(() => {
        setSidebarFilter(ALL_OPTION);
    }, [useApplicationFilter]);

    const visibleProducts = useMemo(() => {
        if (sidebarFilter === ALL_OPTION) return allProducts;
        if (useApplicationFilter) {
            return allProducts.filter((p) => productHasApplication(p, sidebarFilter));
        }
        return allProducts.filter((p) => p.domain?.domain?.urn === sidebarFilter);
    }, [allProducts, sidebarFilter, useApplicationFilter]);

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

    const filterControl =
        filterOptions.length > 0 ? (
            <SimpleSelect
                size="sm"
                width="full"
                showClear={false}
                selectLabelProps={{
                    variant: 'labeled',
                    label: useApplicationFilter ? t('marketplace.filterApplication') : t('marketplace.filterDomain'),
                }}
                options={[{ value: ALL_OPTION, label: t('marketplace.filterAll') }, ...filterOptions]}
                values={[sidebarFilter]}
                onUpdate={(vals) => setSidebarFilter(vals[0] ?? ALL_OPTION)}
                dataTestId={
                    useApplicationFilter
                        ? 'marketplace-sidebar-application-filter'
                        : 'marketplace-sidebar-domain-filter'
                }
            />
        ) : null;

    const emptyState =
        visibleProducts.length === 0 ? (
            <EmptyStateWrapper>
                <EmptyState
                    icon={Storefront}
                    title={
                        allProducts.length === 0 ? t('marketplace.emptyTreeTitle') : t('marketplace.emptyFilterTitle')
                    }
                    description={
                        allProducts.length === 0
                            ? t('marketplace.emptyTreeDescription')
                            : t('marketplace.emptyFilterDescription')
                    }
                    size="sm"
                />
            </EmptyStateWrapper>
        ) : null;

    return (
        <HierarchicalBrowseSidebar
            title={t('marketplace.sidebarTitle')}
            isCollapsed={isCollapsed}
            onToggleCollapsed={onToggleCollapsed}
            onExpandSidebar={onExpandSidebar}
            dataTestId="marketplace-sidebar"
            collapseButtonTestId="marketplace-sidebar-collapse-button"
            collapsedSearchAriaLabel={t('marketplace.searchAriaLabel')}
            collapsedSearchTestId="marketplace-sidebar-search-icon"
            search={<MarketplaceSearch />}
            filters={filterControl}
            homeNav={
                <SidebarHomeNavLink
                    to={PageRoutes.MARKETPLACE}
                    isSelected={isHomeSelected}
                    label={t('marketplace.allDataProducts')}
                    data-testid="marketplace-sidebar-home"
                />
            }
        >
            <div data-testid="marketplace-sidebar-tree">
                <TreeSectionHeader
                    level={0}
                    label={t('marketplace.dataProductsSection')}
                    isExpanded={isProductsExpanded}
                    onToggle={() => setIsProductsExpanded((v) => !v)}
                    onToggleExpandAll={handleToggleExpandAll}
                    isAllExpanded={isSectionExpanded}
                    expandAllLabel={t('marketplace.expandAll')}
                    collapseAllLabel={t('marketplace.collapseAll')}
                    testId="marketplace-sidebar-products-section"
                />
                {isProductsExpanded && (
                    <>
                        {emptyState}
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
        </HierarchicalBrowseSidebar>
    );
}

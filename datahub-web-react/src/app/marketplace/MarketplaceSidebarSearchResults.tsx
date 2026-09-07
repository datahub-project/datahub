import { Storefront } from '@phosphor-icons/react/dist/csr/Storefront';
import React from 'react';
import { useHistory } from 'react-router-dom';

import { MarketplaceTreeItem } from '@app/marketplace/MarketplaceTreeItem';
import { DataProductEntity } from '@app/marketplace/marketplaceTypes';
import SidebarFilteredResults from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/SidebarFilteredResults';
import { PageRoutes } from '@conf/Global';

type Props = {
    dataProducts: DataProductEntity[];
    /** Index total (may be > dataProducts.length when more pages are available). */
    total: number;
    loading: boolean;
    isRefreshing?: boolean;
    selectedUrn?: string | null;
    scrollRef?: (node?: Element | null) => void;
    onClear: () => void;
};

/**
 * Flat search results for marketplace sidebar search mode (query and/or filters).
 */
export default function MarketplaceSidebarSearchResults({
    dataProducts,
    total,
    loading,
    isRefreshing = false,
    selectedUrn,
    scrollRef,
    onClear,
}: Props) {
    const history = useHistory();

    return (
        <SidebarFilteredResults
            count={dataProducts.length}
            total={total}
            loading={loading}
            isRefreshing={isRefreshing}
            onClear={onClear}
            clearTestId="marketplace-sidebar-clear-search"
            dataTestId="marketplace-sidebar-search-results"
        >
            {dataProducts.map((product) => {
                const title = product.properties?.name ?? product.urn;
                return (
                    <MarketplaceTreeItem
                        key={product.urn}
                        level={0}
                        icon={Storefront}
                        title={title}
                        isSelected={product.urn === selectedUrn}
                        hasChildren={false}
                        isExpanded={false}
                        onClick={() =>
                            history.push(`${PageRoutes.DATA_PRODUCT_ENTITY}/${encodeURIComponent(product.urn)}`)
                        }
                        testId={`marketplace-sidebar-search-product-${product.urn}`}
                    />
                );
            })}
            {scrollRef ? <div ref={scrollRef} style={{ height: 1 }} /> : null}
        </SidebarFilteredResults>
    );
}

import { Storefront } from '@phosphor-icons/react/dist/csr/Storefront';
import React, { useEffect } from 'react';
import { useHistory } from 'react-router-dom';

import { MarketplaceTreeItem } from '@app/marketplace/MarketplaceTreeItem';
import { useMarketplaceEntityContext } from '@app/marketplace/context/MarketplaceEntityContext';
import { DataProductEntity } from '@app/marketplace/marketplaceTypes';
import useDataProductChildren from '@app/marketplace/useDataProductChildren';
import { PageRoutes } from '@conf/Global';

export type DataProductChildRowProps = {
    level: number;
    dataProduct: DataProductEntity;
    isExpanded: boolean;
    isSelected: boolean;
    expandedDataProductUrns: Set<string>;
    selectedUrn: string | null;
    onToggle: () => void;
    onToggleDataProduct: (urn: string) => void;
};

export function DataProductChildRow({
    level,
    dataProduct,
    isExpanded,
    isSelected,
    expandedDataProductUrns,
    selectedUrn,
    onToggle,
    onToggleDataProduct,
}: DataProductChildRowProps) {
    const history = useHistory();
    const { entityData } = useMarketplaceEntityContext();
    const hasChildren = (dataProduct.childDataProducts?.total ?? 0) > 0;
    const childCount = dataProduct.childDataProducts?.total ?? 0;

    useEffect(() => {
        if (!entityData?.parentDataProducts) return;
        if (entityData.parentDataProducts.some((p) => p.urn === dataProduct.urn) && !isExpanded) {
            onToggle();
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [entityData?.urn, entityData?.parentDataProducts, dataProduct.urn]);

    const { data, scrollRef } = useDataProductChildren({
        parentUrn: dataProduct.urn,
        skip: !isExpanded || !hasChildren,
    });

    const children = data as DataProductEntity[];
    const title = dataProduct.properties?.name ?? dataProduct.urn;

    return (
        <>
            <MarketplaceTreeItem
                level={level}
                icon={Storefront}
                title={title}
                isSelected={isSelected}
                hasChildren={hasChildren}
                childCount={childCount}
                isExpanded={isExpanded}
                onClick={() => history.push(`${PageRoutes.DATA_PRODUCT_ENTITY}/${encodeURIComponent(dataProduct.urn)}`)}
                onToggleExpand={hasChildren ? onToggle : undefined}
                testId={`marketplace-sidebar-product-${dataProduct.urn}`}
            />
            {isExpanded &&
                children.map((child) => (
                    <DataProductChildRow
                        key={child.urn}
                        level={level + 1}
                        dataProduct={child}
                        isExpanded={expandedDataProductUrns.has(child.urn)}
                        isSelected={selectedUrn === child.urn}
                        expandedDataProductUrns={expandedDataProductUrns}
                        selectedUrn={selectedUrn}
                        onToggle={() => onToggleDataProduct(child.urn)}
                        onToggleDataProduct={onToggleDataProduct}
                    />
                ))}
            {isExpanded && <div ref={scrollRef} style={{ height: 1 }} />}
        </>
    );
}

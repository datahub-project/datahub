import { DataProductEntity } from '@app/marketplace/marketplaceTypes';

import { DataProduct } from '@types';

/** Shape returned by browse queries; used for optimistic list updates after create. */
export function toMarketplaceDataProductEntity(dataProduct: DataProduct): DataProductEntity {
    return {
        __typename: 'DataProduct',
        urn: dataProduct.urn,
        type: dataProduct.type,
        properties: dataProduct.properties,
        parentDataProducts: dataProduct.parentDataProducts ?? null,
        domain: dataProduct.domain ?? null,
        applications: dataProduct.applications ?? null,
        ownership: dataProduct.ownership ?? null,
        tags: dataProduct.tags ?? null,
        deprecation: dataProduct.deprecation ?? null,
        childDataProducts: dataProduct.childDataProducts ?? { total: 0 },
    } as DataProductEntity;
}

export function isRootDataProduct(product: DataProductEntity): boolean {
    return !product.properties?.parentDataProduct?.urn;
}

export function mergeDataProductEntities(
    fetched: DataProductEntity[],
    optimistic: DataProductEntity[],
): DataProductEntity[] {
    if (optimistic.length === 0) return fetched;
    const fetchedUrns = new Set(fetched.map((p) => p.urn));
    const pending = optimistic.filter((p) => !fetchedUrns.has(p.urn));
    if (pending.length === 0) return fetched;
    return [...pending, ...fetched];
}

/** Optimistic rows not yet present in a fetched page (for stat cards). */
export function countPendingOptimistic<T extends { urn: string }>(fetched: T[], optimistic: T[]): number {
    if (optimistic.length === 0) return 0;
    const fetchedUrns = new Set(fetched.map((entity) => entity.urn));
    return optimistic.filter((entity) => !fetchedUrns.has(entity.urn)).length;
}

/** Drop optimistic rows once search/index returns them. */
export function pruneOptimisticProducts(
    optimistic: DataProductEntity[],
    indexedUrns: Iterable<string>,
): DataProductEntity[] {
    const indexed = indexedUrns instanceof Set ? indexedUrns : new Set(indexedUrns);
    if (indexed.size === 0 || optimistic.length === 0) return optimistic;
    const pruned = optimistic.filter((product) => !indexed.has(product.urn));
    return pruned.length === optimistic.length ? optimistic : pruned;
}

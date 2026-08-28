import { DataProductEntity } from '@app/marketplace/marketplaceTypes';

import { EntityType } from '@types';

type SelectedEntityContext = {
    entityType?: EntityType;
    urn?: string;
    parentDataProducts?: Array<{ urn: string }> | null;
};

/**
 * When the selected data product is missing from the loaded root list (e.g. nested
 * product or index lag), return the root URN to fetch so the sidebar can still show it.
 */
export function resolveMarketplaceFallbackRootUrn(
    entityData: SelectedEntityContext | null | undefined,
    rootProducts: ReadonlyArray<{ urn: string }>,
    isSearchActive: boolean,
): string | null {
    if (isSearchActive || entityData?.entityType !== EntityType.DataProduct || !entityData.urn) {
        return null;
    }

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
}

/** Prepends fallback fetch results that are not already present in the root list. */
export function mergeMarketplaceVisibleRootProducts(
    rootProducts: DataProductEntity[],
    fallbackProducts: DataProductEntity[],
): DataProductEntity[] {
    if (fallbackProducts.length === 0) {
        return rootProducts;
    }
    const existingUrns = new Set(rootProducts.map((p) => p.urn));
    const additions = fallbackProducts.filter((p) => !existingUrns.has(p.urn));
    return additions.length > 0 ? [...additions, ...rootProducts] : rootProducts;
}

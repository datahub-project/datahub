import React, { createContext, useCallback, useContext, useMemo, useRef, useState } from 'react';
import { matchPath, useLocation } from 'react-router-dom';

import { PageRoutes } from '@conf/Global';

import { EntityType } from '@types';

/** Minimal entity data needed by the sidebar to auto-expand the tree to the active entity. */
export type MarketplaceEntityData = {
    urn: string;
    entityType: EntityType;
    /** Ancestor data products from immediate parent to root, nearest first. */
    parentDataProducts?: Array<{ urn: string }> | null;
};

type MarketplaceEntityContextType = {
    expandedDataProductUrns: Set<string>;
    selectedUrn: string | null;
    toggleDataProduct: (urn: string) => void;
    /** Expand every data product in `urns` (union with current). */
    expandAllDataProducts: (urns: string[]) => void;
    /** Collapse every expanded data product. */
    collapseAllExpanded: () => void;
    /** Signal the sidebar to refetch root + all expanded children. */
    refetchTree: () => void;
    /** Incremented each time refetchTree() is called; consumers subscribe via useEffect. */
    refetchKey: number;
    /** Entity currently viewed in the profile pane; null on the /marketplace home page. */
    entityData: MarketplaceEntityData | null;
    setEntityData: (data: MarketplaceEntityData | null) => void;
};

const MarketplaceEntityContext = createContext<MarketplaceEntityContextType | undefined>(undefined);

type Props = {
    children: React.ReactNode;
};

function toggleInSet(prev: Set<string>, value: string): Set<string> {
    const next = new Set(prev);
    if (next.has(value)) {
        next.delete(value);
    } else {
        next.add(value);
    }
    return next;
}

export function MarketplaceEntityContextProvider({ children }: Props) {
    const location = useLocation();
    const [expandedDataProductUrns, setExpandedDataProductUrns] = useState<Set<string>>(new Set());
    const [refetchKey, setRefetchKey] = useState(0);
    const [entityData, setEntityData] = useState<MarketplaceEntityData | null>(null);

    const selectedUrn = useMemo(() => {
        const match = matchPath<{ urn: string }>(location.pathname, {
            path: `${PageRoutes.DATA_PRODUCT_ENTITY}/:urn`,
        });
        if (!match) return null;
        try {
            return decodeURIComponent(match.params.urn);
        } catch {
            return null;
        }
    }, [location.pathname]);

    const toggleDataProduct = useCallback((urn: string) => {
        setExpandedDataProductUrns((prev) => toggleInSet(prev, urn));
    }, []);

    const expandAllDataProducts = useCallback((urns: string[]) => {
        setExpandedDataProductUrns((prev) => {
            const next = new Set(prev);
            urns.forEach((urn) => next.add(urn));
            return next;
        });
    }, []);

    const collapseAllExpanded = useCallback(() => {
        setExpandedDataProductUrns(new Set());
    }, []);

    const refetchKeyRef = useRef(0);
    const refetchTree = useCallback(() => {
        refetchKeyRef.current += 1;
        setRefetchKey(refetchKeyRef.current);
    }, []);

    const value = useMemo(
        () => ({
            expandedDataProductUrns,
            selectedUrn,
            toggleDataProduct,
            expandAllDataProducts,
            collapseAllExpanded,
            refetchTree,
            refetchKey,
            entityData,
            setEntityData,
        }),
        [
            expandedDataProductUrns,
            selectedUrn,
            toggleDataProduct,
            expandAllDataProducts,
            collapseAllExpanded,
            refetchTree,
            refetchKey,
            entityData,
        ],
    );

    return <MarketplaceEntityContext.Provider value={value}>{children}</MarketplaceEntityContext.Provider>;
}

/** Returns undefined when rendered outside MarketplaceEntityContextProvider. */
export function useMaybeMarketplaceEntityContext(): MarketplaceEntityContextType | undefined {
    return useContext(MarketplaceEntityContext);
}

export function useMarketplaceEntityContext(): MarketplaceEntityContextType {
    const context = useMaybeMarketplaceEntityContext();
    if (context === undefined) {
        throw new Error('useMarketplaceEntityContext must be used inside a MarketplaceEntityContextProvider');
    }
    return context;
}

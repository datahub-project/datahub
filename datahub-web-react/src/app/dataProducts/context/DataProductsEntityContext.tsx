import React, { createContext, useCallback, useContext, useEffect, useMemo, useRef, useState } from 'react';
import { matchPath, useHistory, useLocation } from 'react-router-dom';

import CreateDataProductModal from '@app/dataProducts/CreateDataProductModal';
import { DataProductEntity } from '@app/dataProducts/dataProductsTypes';
import {
    pruneOptimisticProducts,
    toDataProductsDataProductEntity,
} from '@app/dataProducts/utils/dataProductsDataProductEntity';
import { PageRoutes } from '@conf/Global';

import { DataProduct, EntityType } from '@types';

/** Delay before a follow-up tree refetch while search indexes a newly created product. */
export const DATA_PRODUCTS_POST_CREATE_REFETCH_MS = 3000;

/** Minimal entity data needed by the sidebar to auto-expand the tree to the active entity. */
export type DataProductsEntityData = {
    urn: string;
    entityType: EntityType;
    /** Ancestor data products from immediate parent to root, nearest first. */
    parentDataProducts?: Array<{ urn: string }> | null;
};

type DataProductsEntityContextType = {
    expandedDataProductUrns: Set<string>;
    selectedUrn: string | null;
    toggleDataProduct: (urn: string) => void;
    /** Expand every data product in `urns` (union with current). */
    expandAllDataProducts: (urns: string[]) => void;
    /** Collapse every expanded data product. */
    collapseAllExpanded: () => void;
    /** Incremented to signal the sidebar to refetch root + expanded children. */
    refetchKey: number;
    /** Entity currently viewed in the profile pane; null on the /dataProducts home page. */
    entityData: DataProductsEntityData | null;
    setEntityData: (data: DataProductsEntityData | null) => void;
    /** Optimistically inserted products while search indexes the new entity. */
    optimisticDataProducts: DataProductEntity[];
    /** Children to show under a parent before search catches up. */
    getOptimisticChildren: (parentUrn: string) => DataProductEntity[];
    /** Drop optimistic rows once browse/search results include their URNs. */
    syncOptimisticWithIndexed: (indexedUrns: string[]) => void;
    openCreateModal: () => void;
};

const DataProductsEntityContext = createContext<DataProductsEntityContextType | undefined>(undefined);

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

export function DataProductsEntityContextProvider({ children }: Props) {
    const location = useLocation();
    const history = useHistory();
    const [expandedDataProductUrns, setExpandedDataProductUrns] = useState<Set<string>>(new Set());
    const [refetchKey, setRefetchKey] = useState(0);
    const [entityData, setEntityData] = useState<DataProductsEntityData | null>(null);
    const [isCreateModalVisible, setIsCreateModalVisible] = useState(false);
    const [optimisticDataProducts, setOptimisticDataProducts] = useState<DataProductEntity[]>([]);
    const delayedRefetchRef = useRef<ReturnType<typeof setTimeout>>();

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
    const scheduleRefetchTree = useCallback(() => {
        refetchKeyRef.current += 1;
        setRefetchKey(refetchKeyRef.current);
    }, []);

    const openCreateModal = useCallback(() => {
        setIsCreateModalVisible(true);
    }, []);

    const closeCreateModal = useCallback(() => {
        setIsCreateModalVisible(false);
    }, []);

    const registerCreatedDataProduct = useCallback((dataProduct: DataProduct) => {
        const entity = toDataProductsDataProductEntity(dataProduct);
        setOptimisticDataProducts((prev) => {
            if (prev.some((p) => p.urn === entity.urn)) return prev;
            return [entity, ...prev];
        });
    }, []);

    const syncOptimisticWithIndexed = useCallback((indexedUrns: string[]) => {
        setOptimisticDataProducts((prev) => pruneOptimisticProducts(prev, indexedUrns));
    }, []);

    const getOptimisticChildren = useCallback(
        (parentUrn: string) => optimisticDataProducts.filter((p) => p.properties?.parentDataProduct?.urn === parentUrn),
        [optimisticDataProducts],
    );

    const handleCreateDataProduct = useCallback(
        (dataProduct: DataProduct) => {
            registerCreatedDataProduct(dataProduct);
            const parentUrn = dataProduct.properties?.parentDataProduct?.urn;
            if (parentUrn) {
                expandAllDataProducts([parentUrn]);
            }
            scheduleRefetchTree();
            if (delayedRefetchRef.current) {
                clearTimeout(delayedRefetchRef.current);
            }
            delayedRefetchRef.current = setTimeout(() => scheduleRefetchTree(), DATA_PRODUCTS_POST_CREATE_REFETCH_MS);
            history.push(`${PageRoutes.DATA_PRODUCT_ENTITY}/${encodeURIComponent(dataProduct.urn)}`);
        },
        [expandAllDataProducts, history, registerCreatedDataProduct, scheduleRefetchTree],
    );

    useEffect(
        () => () => {
            if (delayedRefetchRef.current) {
                clearTimeout(delayedRefetchRef.current);
            }
        },
        [],
    );

    const value = useMemo(
        () => ({
            expandedDataProductUrns,
            selectedUrn,
            toggleDataProduct,
            expandAllDataProducts,
            collapseAllExpanded,
            refetchKey,
            entityData,
            setEntityData,
            optimisticDataProducts,
            getOptimisticChildren,
            syncOptimisticWithIndexed,
            openCreateModal,
        }),
        [
            expandedDataProductUrns,
            selectedUrn,
            toggleDataProduct,
            expandAllDataProducts,
            collapseAllExpanded,
            refetchKey,
            entityData,
            optimisticDataProducts,
            getOptimisticChildren,
            syncOptimisticWithIndexed,
            openCreateModal,
        ],
    );

    return (
        <DataProductsEntityContext.Provider value={value}>
            {children}
            <CreateDataProductModal
                open={isCreateModalVisible}
                onClose={closeCreateModal}
                onCreateDataProduct={handleCreateDataProduct}
            />
        </DataProductsEntityContext.Provider>
    );
}

/** Returns undefined when rendered outside DataProductsEntityContextProvider. */
export function useMaybeDataProductsEntityContext(): DataProductsEntityContextType | undefined {
    return useContext(DataProductsEntityContext);
}

export function useDataProductsEntityContext(): DataProductsEntityContextType {
    const context = useMaybeDataProductsEntityContext();
    if (context === undefined) {
        throw new Error('useDataProductsEntityContext must be used inside a DataProductsEntityContextProvider');
    }
    return context;
}

import { useApolloClient } from '@apollo/client';
import { useCallback, useMemo, useState } from 'react';

import {
    DATA_PRODUCT_TREE_PAGE_SIZE,
    getDataProductChildrenScrollInput,
    getDataProductRootsScrollInput,
} from '@app/entityV2/shared/EntityDropdown/dataProductTreeUtils';
import { DataProductEntity } from '@app/marketplace/marketplaceTypes';

import {
    ScrollDataProductsDocument,
    ScrollDataProductsQuery,
    ScrollDataProductsQueryVariables,
    useScrollDataProductsQuery,
} from '@graphql/marketplaceBrowse.generated';
import { DataProduct, EntityType } from '@types';

type UseDataProductTreeEntitiesResult = {
    entities: DataProduct[];
    entityCache: Record<string, DataProduct>;
    expandedNodes: Set<string>;
    fetchingNodes: Set<string>;
    expandNode: (nodeUrn: string) => void;
    collapseNode: (nodeUrn: string) => void;
    isLoading: boolean;
};

function asDataProduct(entity: DataProductEntity | DataProduct): DataProduct {
    return entity as DataProduct;
}

/** Attach a synthetic ancestor chain (direct-parent first) for tree option depth. */
function withParentChain(entity: DataProduct, parentChain: DataProduct[]): DataProduct {
    const directParent = parentChain[0];
    return {
        ...entity,
        parentDataProducts: parentChain,
        properties: {
            ...entity.properties,
            name: entity.properties?.name,
            parentDataProduct: directParent
                ? {
                      urn: directParent.urn,
                      type: EntityType.DataProduct,
                      properties: { name: directParent.properties?.name },
                  }
                : entity.properties?.parentDataProduct,
        },
    } as DataProduct;
}

/**
 * Data layer for the parent data-product picker — mirrors `useGlossaryTreeEntities`.
 * Loads root products, then lazy-fetches children on expand with per-node loading + cache.
 */
export function useDataProductTreeEntities(): UseDataProductTreeEntitiesResult {
    const apolloClient = useApolloClient();

    const { data: rootScrollData, loading: rootsLoading } = useScrollDataProductsQuery({
        variables: getDataProductRootsScrollInput(null),
        fetchPolicy: 'cache-and-network',
    });

    const [childrenByNode, setChildrenByNode] = useState<Record<string, DataProduct[]>>({});
    const [expandedNodes, setExpandedNodes] = useState<Set<string>>(new Set());
    const [fetchingNodes, setFetchingNodes] = useState<Set<string>>(new Set());
    const [chainByNode, setChainByNode] = useState<Record<string, DataProduct[]>>({});

    const rootProducts = useMemo<DataProduct[]>(() => {
        const results = rootScrollData?.scrollAcrossEntities?.searchResults ?? [];
        return results
            .map((r) => r.entity)
            .filter((e): e is DataProductEntity => e?.__typename === 'DataProduct')
            .map((e) => withParentChain(asDataProduct(e), []));
    }, [rootScrollData]);

    const entityCache = useMemo<Record<string, DataProduct>>(() => {
        const cache: Record<string, DataProduct> = {};
        rootProducts.forEach((p) => {
            cache[p.urn] = p;
        });
        Object.values(childrenByNode).forEach((bucket) => {
            bucket.forEach((e) => {
                cache[e.urn] = e;
            });
        });
        return cache;
    }, [rootProducts, childrenByNode]);

    const expandNode = useCallback(
        (nodeUrn: string) => {
            setExpandedNodes((prev) => {
                if (prev.has(nodeUrn)) return prev;
                const next = new Set(prev);
                next.add(nodeUrn);
                return next;
            });
            if (childrenByNode[nodeUrn] || fetchingNodes.has(nodeUrn)) return;

            setFetchingNodes((prev) => {
                const next = new Set(prev);
                next.add(nodeUrn);
                return next;
            });

            const parentChainForNode = chainByNode[nodeUrn] ?? [];
            const parentEntity = entityCache[nodeUrn];
            const childChain: DataProduct[] = parentEntity ? [parentEntity, ...parentChainForNode] : parentChainForNode;

            apolloClient
                .query<ScrollDataProductsQuery, ScrollDataProductsQueryVariables>({
                    query: ScrollDataProductsDocument,
                    variables: getDataProductChildrenScrollInput(nodeUrn, null, DATA_PRODUCT_TREE_PAGE_SIZE),
                })
                .then(({ data }) => {
                    const children = (data.scrollAcrossEntities?.searchResults ?? [])
                        .map((r) => r.entity)
                        .filter((e): e is DataProductEntity => e?.__typename === 'DataProduct')
                        .map((e) => withParentChain(asDataProduct(e), childChain));

                    setChildrenByNode((prev) => ({ ...prev, [nodeUrn]: children }));
                    setChainByNode((prev) => {
                        const next = { ...prev };
                        children.forEach((c) => {
                            next[c.urn] = childChain;
                        });
                        return next;
                    });
                })
                .finally(() => {
                    setFetchingNodes((prev) => {
                        const next = new Set(prev);
                        next.delete(nodeUrn);
                        return next;
                    });
                });
        },
        [apolloClient, childrenByNode, chainByNode, fetchingNodes, entityCache],
    );

    const collapseNode = useCallback((nodeUrn: string) => {
        setExpandedNodes((prev) => {
            if (!prev.has(nodeUrn)) return prev;
            const next = new Set(prev);
            next.delete(nodeUrn);
            return next;
        });
    }, []);

    const entities = useMemo<DataProduct[]>(() => {
        const out: DataProduct[] = [...rootProducts];
        expandedNodes.forEach((urn) => {
            const cached = childrenByNode[urn];
            if (cached) out.push(...cached);
        });
        return out;
    }, [rootProducts, expandedNodes, childrenByNode]);

    return {
        entities,
        entityCache,
        expandedNodes,
        fetchingNodes,
        expandNode,
        collapseNode,
        isLoading: rootsLoading,
    };
}

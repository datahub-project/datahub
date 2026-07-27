import { useContext, useEffect, useState } from 'react';

import {
    DATA_PRODUCT_MEMBER_PAGE_SIZE,
    FetchStatus,
    LineageEntity,
    LineageNodesContext,
    setDefault,
} from '@app/lineageV3/common';

import { useGetDataProductEntitiesForLineageQuery } from '@graphql/dataProduct.generated';
import { Entity, LineageDirection } from '@types';

/**
 * Fetches the entities belonging to a DataProduct and registers them as lineage nodes, in pages of
 * `DATA_PRODUCT_MEMBER_PAGE_SIZE`. Only fetches up to the home box's `boundingBoxLimit`, which the
 * "Show more" control raises a page at a time — so large data products aren't loaded eagerly. Each
 * entity node is initialised as expanded and fetched: their first-hop lineage is loaded by
 * `useBulkEntityLineage` (via the upstream/downstream fields in entityLineageV2), so no separate
 * lineage fetch is needed.
 */
export default function useFetchDataProductEntities(): boolean {
    const { rootUrn, nodes, setNodeVersion } = useContext(LineageNodesContext);
    const [start, setStart] = useState(0);
    const [total, setTotal] = useState<number | undefined>(undefined);
    const [initialized, setInitialized] = useState(false);
    const limit = nodes.get(rootUrn)?.boundingBoxLimit ?? DATA_PRODUCT_MEMBER_PAGE_SIZE;

    useGetDataProductEntitiesForLineageQuery({
        variables: {
            urn: rootUrn,
            start,
            count: DATA_PRODUCT_MEMBER_PAGE_SIZE,
        },
        onCompleted: (data) => {
            let addedNode = false;

            const entities = data.dataProduct?.entities;
            entities?.searchResults?.forEach((result) => {
                if (!result?.entity) return;
                addedNode = addedNode || !nodes.has(result.entity.urn);
                // Membership (including the home product) is resolved by useBulkDataProductMemberships
                setDefault(nodes, result.entity.urn, makeEntityNode(result.entity));
            });

            if (entities?.total !== undefined) setTotal(entities.total);
            setInitialized(true);

            if (addedNode) setNodeVersion((version) => version + 1);
        },
    });

    // Advance one page at a time until we've fetched up to the current limit (or run out of members).
    // Runs on limit increases too, so clicking "Show more" pulls in the next page.
    const target = Math.min(limit, total ?? limit);
    useEffect(() => {
        if (start + DATA_PRODUCT_MEMBER_PAGE_SIZE < target) {
            setStart((prev) => prev + DATA_PRODUCT_MEMBER_PAGE_SIZE);
        }
    }, [start, target]);

    return initialized;
}

function makeEntityNode({ urn, type }: Entity): LineageEntity {
    return {
        id: urn,
        urn,
        type,
        // Auto-expand so that `computeDataProductGraph` shows 1-hop external nodes and
        // neighboring DataProduct bounding boxes without requiring manual user expansion.
        // The lineage data for these first-hop nodes is already populated by `processEdge`
        // in `useBulkEntityLineage` (via the upstream/downstream fields in entityLineageV2), so
        // no additional queries are triggered here and fetch status starts COMPLETE, making
        // these nodes present as expanded.
        isExpanded: {
            [LineageDirection.Upstream]: true,
            [LineageDirection.Downstream]: true,
        },
        fetchStatus: {
            [LineageDirection.Upstream]: FetchStatus.COMPLETE,
            [LineageDirection.Downstream]: FetchStatus.COMPLETE,
        },
        filters: {
            [LineageDirection.Upstream]: { limit: undefined, facetFilters: new Map() },
            [LineageDirection.Downstream]: { limit: undefined, facetFilters: new Map() },
        },
    };
}

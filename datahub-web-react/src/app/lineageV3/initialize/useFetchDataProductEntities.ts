import { useContext, useState } from 'react';

import { FetchStatus, LineageEntity, LineageNodesContext, setDefault } from '@app/lineageV3/common';

import { useGetDataProductEntitiesForLineageQuery } from '@graphql/dataProduct.generated';
import { Entity, LineageDirection } from '@types';

const DATA_PRODUCT_ENTITIES_PAGE_SIZE = 50;

/**
 * Fetches the entities belonging to a DataProduct and registers them as lineage nodes.
 * Each entity node is initialised with UNFETCHED status so that the standard lineage
 * expansion mechanism will fetch their own upstream/downstream edges on demand.
 */
export default function useFetchDataProductEntities(): boolean {
    const { rootUrn, nodes, setNodeVersion } = useContext(LineageNodesContext);
    const [start, setStart] = useState(0);
    const [initialized, setInitialized] = useState(false);

    useGetDataProductEntitiesForLineageQuery({
        variables: {
            urn: rootUrn,
            start,
            count: DATA_PRODUCT_ENTITIES_PAGE_SIZE,
        },
        onCompleted: (data) => {
            let addedNode = false;

            const entities = data.dataProduct?.entities;
            entities?.searchResults?.forEach((result) => {
                if (!result?.entity) return;
                addedNode = addedNode || !nodes.has(result.entity.urn);
                const node = setDefault(nodes, result.entity.urn, makeEntityNode(result.entity));
                node.parentDataProduct = rootUrn;
            });

            if (entities?.total && entities.total > start + DATA_PRODUCT_ENTITIES_PAGE_SIZE) {
                setStart((prev) => prev + DATA_PRODUCT_ENTITIES_PAGE_SIZE);
            }

            setInitialized(true);

            if (addedNode) setNodeVersion((version) => version + 1);
        },
    });

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
        // in `useBulkEntityLineage` (via the upstream/downstream fields in entityLineageV2),
        // so no additional queries are triggered here.
        isExpanded: {
            [LineageDirection.Upstream]: true,
            [LineageDirection.Downstream]: true,
        },
        fetchStatus: {
            [LineageDirection.Upstream]: FetchStatus.UNFETCHED,
            [LineageDirection.Downstream]: FetchStatus.UNFETCHED,
        },
        filters: {
            [LineageDirection.Upstream]: { limit: undefined, facetFilters: new Map() },
            [LineageDirection.Downstream]: { limit: undefined, facetFilters: new Map() },
        },
    };
}

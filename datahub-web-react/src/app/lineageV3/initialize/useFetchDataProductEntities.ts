import { useContext } from 'react';

import { BOUNDING_BOX_MEMBER_PAGE_SIZE, LineageNodesContext, setDefault } from '@app/lineageV3/common';
import {
    createBoundingBoxMemberNode,
    useBoundingBoxMemberPagination,
} from '@app/lineageV3/initialize/initialize.utils';

import { useGetDataProductEntitiesForLineageQuery } from '@graphql/dataProduct.generated';

/**
 * Fetches the entities belonging to a DataProduct and registers them as lineage nodes, in pages of
 * `BOUNDING_BOX_MEMBER_PAGE_SIZE`. Only fetches up to the home box's `boundingBoxLimit`, which the
 * "Show more" control raises a page at a time — so large data products aren't loaded eagerly. Each
 * entity node is initialised as expanded and fetched: their first-hop lineage is loaded by
 * `useBulkEntityLineage` (via the upstream/downstream fields in entityLineageV2), so no separate
 * lineage fetch is needed.
 */
export default function useFetchDataProductEntities(): boolean {
    const { rootUrn, nodes, setNodeVersion } = useContext(LineageNodesContext);
    const { start, setTotal, initialized, setInitialized } = useBoundingBoxMemberPagination(rootUrn, nodes);

    useGetDataProductEntitiesForLineageQuery({
        variables: {
            urn: rootUrn,
            start,
            count: BOUNDING_BOX_MEMBER_PAGE_SIZE,
        },
        onCompleted: (data) => {
            let addedNode = false;

            const entities = data.dataProduct?.entities;
            entities?.searchResults?.forEach((result) => {
                if (!result?.entity) return;
                addedNode = addedNode || !nodes.has(result.entity.urn);
                // Membership (including the home product) is resolved by useBulkDataProductMemberships
                setDefault(nodes, result.entity.urn, createBoundingBoxMemberNode(result.entity));
            });

            if (entities?.total !== undefined) setTotal(entities.total);
            setInitialized(true);

            if (addedNode) setNodeVersion((version) => version + 1);
        },
    });

    return initialized;
}

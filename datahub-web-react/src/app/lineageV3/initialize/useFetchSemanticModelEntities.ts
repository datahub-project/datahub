import { useContext } from 'react';

import { BOUNDING_BOX_MEMBER_PAGE_SIZE, LineageNodesContext, setDefault } from '@app/lineageV3/common';
import {
    createBoundingBoxMemberNode,
    useBoundingBoxMemberPagination,
} from '@app/lineageV3/initialize/initialize.utils';

import { useGetSemanticModelEntitiesForLineageQuery } from '@graphql/semanticModel.generated';

/**
 * Fetches the entities belonging to a SemanticModel (logical datasets + metrics) and
 * registers them as lineage nodes, in pages of `BOUNDING_BOX_MEMBER_PAGE_SIZE`. Marks
 * each member with `boundingBoxes: [{ urn: rootUrn }]` so the shared bounding-box compute
 * path (computeBoundingBoxGraph) groups them inside the home SemanticModel box.
 */
export default function useFetchSemanticModelEntities(): boolean {
    const { rootUrn, nodes, boundingBoxEntities, setNodeVersion, setDataVersion } = useContext(LineageNodesContext);
    const { start, setTotal, initialized, setInitialized } = useBoundingBoxMemberPagination(rootUrn, nodes);

    useGetSemanticModelEntitiesForLineageQuery({
        variables: {
            urn: rootUrn,
            start,
            count: BOUNDING_BOX_MEMBER_PAGE_SIZE,
        },
        onCompleted: (data) => {
            let addedNode = false;
            // Track membership upgrades on already-known nodes separately; adding a node bumps
            // nodeVersion (which invalidates useComputeGraph), but re-hydrating a lineage-neighbor's
            // home membership after it was marked free needs its own dataVersion bump — otherwise
            // the recompute never runs and the node stays visually outside the SM box.
            let membershipChanged = false;

            const entities = data.semanticModel?.entities;
            entities?.searchResults?.forEach((result) => {
                if (!result?.entity) return;
                const isNew = !nodes.has(result.entity.urn);
                addedNode = addedNode || isNew;
                const node = setDefault(nodes, result.entity.urn, createBoundingBoxMemberNode(result.entity, rootUrn));
                // Ensure home-membership is recorded even if the node was created earlier.
                if (!node.boundingBoxes?.some((c) => c.urn === rootUrn)) {
                    node.boundingBoxes = [{ urn: rootUrn, isOutputPort: false }, ...(node.boundingBoxes ?? [])];
                    if (!isNew) membershipChanged = true;
                }
            });

            // Store a minimal display entity for the home SemanticModel bounding box,
            // and hydrate the root node so the box label renders.
            if (data.semanticModel) {
                const displayEntity = {
                    urn: rootUrn,
                    type: data.semanticModel.type,
                    name: data.semanticModel.info?.name || rootUrn,
                    exists: data.semanticModel.exists ?? true,
                    icon: data.semanticModel.platform?.properties?.logoUrl || undefined,
                    platform: data.semanticModel.platform ?? undefined,
                };
                if (!boundingBoxEntities.has(rootUrn)) {
                    boundingBoxEntities.set(rootUrn, displayEntity);
                }
                const rootNode = nodes.get(rootUrn);
                if (rootNode && !rootNode.entity) {
                    rootNode.entity = displayEntity;
                    addedNode = true;
                }
            }

            if (entities?.total !== undefined) setTotal(entities.total);
            setInitialized(true);

            if (addedNode) setNodeVersion((version) => version + 1);
            if (membershipChanged) setDataVersion((version) => version + 1);
        },
    });

    return initialized;
}

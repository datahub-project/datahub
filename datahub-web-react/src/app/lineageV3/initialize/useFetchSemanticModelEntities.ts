import { useContext } from 'react';

import { CONTAINER_MEMBER_PAGE_SIZE, LineageNodesContext, setDefault } from '@app/lineageV3/common';
import { createContainerMemberNode, useContainerMemberPagination } from '@app/lineageV3/initialize/initialize.utils';

import { useGetSemanticModelEntitiesForLineageQuery } from '@graphql/semanticModel.generated';

/**
 * Fetches the entities belonging to a SemanticModel (logical datasets + metrics) and
 * registers them as lineage nodes, in pages of `CONTAINER_MEMBER_PAGE_SIZE`. Marks
 * each member with `containers: [{ urn: rootUrn }]` so the shared bounding-box compute
 * path (computeLineageContainerGraph) groups them inside the home SemanticModel box.
 */
export default function useFetchSemanticModelEntities(): boolean {
    const { rootUrn, nodes, containerEntities, setNodeVersion } = useContext(LineageNodesContext);
    const { start, setTotal, initialized, setInitialized } = useContainerMemberPagination(rootUrn, nodes);

    useGetSemanticModelEntitiesForLineageQuery({
        variables: {
            urn: rootUrn,
            start,
            count: CONTAINER_MEMBER_PAGE_SIZE,
        },
        onCompleted: (data) => {
            let addedNode = false;

            const entities = data.semanticModel?.entities;
            entities?.searchResults?.forEach((result) => {
                if (!result?.entity) return;
                addedNode = addedNode || !nodes.has(result.entity.urn);
                const node = setDefault(nodes, result.entity.urn, createContainerMemberNode(result.entity, rootUrn));
                // Ensure home-membership is recorded even if the node was created earlier.
                if (!node.containers?.some((c) => c.urn === rootUrn)) {
                    node.containers = [{ urn: rootUrn, isOutputPort: false }, ...(node.containers ?? [])];
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
                if (!containerEntities.has(rootUrn)) {
                    containerEntities.set(rootUrn, displayEntity);
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
        },
    });

    return initialized;
}

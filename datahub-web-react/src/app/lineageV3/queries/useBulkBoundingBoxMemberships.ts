import { useContext, useEffect } from 'react';

import { LineageNodesContext } from '@app/lineageV3/common';
import useBulkDataProductMemberships from '@app/lineageV3/queries/useBulkDataProductMemberships';

import { EntityType } from '@types';

/**
 * Resolves bounding-box membership for every node in a bounding-box lineage graph.
 *
 * Shared contract for all BOUNDING_BOX_ENTITY_TYPES:
 * - `boundingBoxes === undefined` → membership unknown (node hidden by the graph filter)
 * - `boundingBoxes === []` → known free (not in any box)
 * - `boundingBoxes === [{ urn, isOutputPort }, …]` → known member of those boxes
 *
 * Type-specific loaders fill that field:
 * - DataProduct: fetches via `bulkEntityDataProducts` (neighbor boxes included)
 * - SemanticModel: until a bulk SM membership API exists, marks remaining unknown nodes as
 *   free (`[]`). Home members are already set by `useFetchSemanticModelEntities`.
 */
export default function useBulkBoundingBoxMemberships() {
    useBulkDataProductMemberships();
    useMarkUnknownSemanticModelMembershipsEmpty();
}

/**
 * SemanticModel stand-in for bulk membership: any node still unknown is marked free so the
 * shared hide-until-known filter can show it. Replace with a real bulk fetch when neighbor
 * SemanticModel boxes are supported.
 */
function useMarkUnknownSemanticModelMembershipsEmpty() {
    const { rootType, nodes, nodeVersion, dataVersion, setDataVersion } = useContext(LineageNodesContext);
    const skip = rootType !== EntityType.SemanticModel;

    useEffect(() => {
        if (skip) return;
        let changed = false;
        nodes.forEach((node) => {
            if (node.boundingBoxes === undefined && node.type !== EntityType.Query) {
                // Mutate in place — matches useBulkDataProductMemberships.
                // eslint-disable-next-line no-param-reassign
                node.boundingBoxes = [];
                changed = true;
            }
        });
        if (changed) {
            setDataVersion((version) => version + 1);
        }
    }, [skip, nodes, nodeVersion, dataVersion, setDataVersion]);
}

import { useContext, useEffect } from 'react';

import { LineageNodesContext } from '@app/lineageV3/common';

import { EntityType } from '@types';

/**
 * For the SemanticModel lineage graph, marks every node whose container membership is
 * still unknown with an empty `containers` array so the shared bounding-box compute
 * path can render it as a free (non-boxed) node. Home members already have membership
 * set by `useFetchSemanticModelEntities`.
 *
 * Neighbor SemanticModel boxes (an entity belonging to a different SM) are not resolved
 * in this pass — membership is home-only for the MVP.
 */
export default function useBulkSemanticModelMemberships() {
    const { rootType, nodes, nodeVersion, dataVersion, setDataVersion } = useContext(LineageNodesContext);
    const skip = rootType !== EntityType.SemanticModel;

    useEffect(() => {
        if (skip) return;
        let changed = false;
        nodes.forEach((node) => {
            if (node.containers === undefined && node.type !== EntityType.Query) {
                // Mutate in place — matches useBulkDataProductMemberships.
                // eslint-disable-next-line no-param-reassign
                node.containers = [];
                changed = true;
            }
        });
        if (changed) {
            setDataVersion((version) => version + 1);
        }
    }, [skip, nodes, nodeVersion, dataVersion, setDataVersion]);
}

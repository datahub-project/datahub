/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useContext, useMemo } from 'react';

import { LineageNodesContext } from '@app/lineageV3/common';
import useSearchAcrossLineage from '@app/lineageV3/queries/useSearchAcrossLineage';

import { EntityType, LineageDirection } from '@types';

export default function useRefetchLineage(urn: string, type: EntityType) {
    const nodeContext = useContext(LineageNodesContext);

    const { fetchLineage: fetchLineageUpstream } = useSearchAcrossLineage(
        urn,
        type,
        nodeContext,
        LineageDirection.Upstream,
        true,
        false,
        true,
    );
    const { fetchLineage: fetchLineageDownstream } = useSearchAcrossLineage(
        urn,
        type,
        nodeContext,
        LineageDirection.Downstream,
        true,
        false,
        true,
    );

    return useMemo(
        () => ({
            [LineageDirection.Upstream]: () => {
                const timeout = setTimeout(fetchLineageUpstream, 7000);
                return () => clearTimeout(timeout);
            },
            [LineageDirection.Downstream]: () => {
                const timeout = setTimeout(fetchLineageDownstream, 7000);
                return () => clearTimeout(timeout);
            },
        }),
        [fetchLineageUpstream, fetchLineageDownstream],
    );
}

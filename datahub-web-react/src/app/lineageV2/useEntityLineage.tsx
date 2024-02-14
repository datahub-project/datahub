import { useContext, useEffect, useMemo } from 'react';
import { useGetLineageTimeParams } from '../lineage/utils/useGetLineageTimeParams';
import { useEntityRegistryV2 } from '../useEntityRegistry';
import { FetchStatus, LineageNodesContext } from './common';
import { LineageDirection } from '../../types.generated';
import { FetchedEntityV2 } from './types';
import { useGetEntityLineageV2Query } from '../../graphql/lineage.generated';

/**
 * Fetches the data and detailed lineage for a given urn
 * @param urn Urn for which to fetch lineage
 */
export default function useEntityLineage(urn: string): FetchedEntityV2 | undefined {
    const { nodes, setDataVersion } = useContext(LineageNodesContext);
    const { startTimeMillis, endTimeMillis } = useGetLineageTimeParams();
    const node = nodes.get(urn);

    const { data } = useGetEntityLineageV2Query({
        skip: !node,
        fetchPolicy: 'cache-first',
        variables: {
            urn,
            startTimeMillis,
            endTimeMillis,
            separateSiblings: false,
            showColumns: true,
            excludeUpstream: node?.fetchStatus[LineageDirection.Upstream] === FetchStatus.UNNEEDED,
            excludeDownstream: node?.fetchStatus[LineageDirection.Downstream] === FetchStatus.UNNEEDED,
        },
    });

    const entityRegistry = useEntityRegistryV2();

    const entityDetails = useMemo(
        () =>
            data?.entity && {
                ...entityRegistry.getLineageVizConfig(data.entity.type, data.entity),
                lineageAssets: entityRegistry.getLineageAssets(data.entity.type, data.entity),
            },
        [data, entityRegistry],
    );
    useEffect(() => {
        if (node && entityDetails) {
            node.entity = entityDetails;
            setDataVersion((version) => version + 1);
        }
    }, [node, entityDetails, entityRegistry, setDataVersion]);

    return entityDetails || undefined;
}

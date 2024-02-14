import { useContext, useEffect, useMemo, useState } from 'react';
import { useGetBulkEntityLineageV2Query } from '../../graphql/lineage.generated';
import { useGetLineageTimeParams } from '../lineage/utils/useGetLineageTimeParams';
import { useEntityRegistryV2 } from '../useEntityRegistry';
import { FetchStatus, LineageNodesContext } from './common';
import { LineageDirection } from '../../types.generated';
import usePrevious from '../shared/usePrevious';
import { FetchedEntityV2 } from './types';

export default function useBulkEntityLineage(shownUrns: string[], direction: LineageDirection | null): void {
    const { nodes, setDataVersion } = useContext(LineageNodesContext);
    shownUrns.sort();
    const prevShownUrns = usePrevious(shownUrns);
    const [urnsToFetch, setUrnsToFetch] = useState<string[]>([]);
    useEffect(() => {
        // TODO: Implement string[] equality?
        if (JSON.stringify(prevShownUrns) !== JSON.stringify(shownUrns)) {
            setUrnsToFetch(
                shownUrns.filter((urn) => {
                    const node = nodes.get(urn);
                    return (
                        !node?.entity &&
                        (direction
                            ? node?.fetchStatus[direction] !== FetchStatus.UNNEEDED
                            : node?.fetchStatus[LineageDirection.Upstream] === FetchStatus.UNNEEDED &&
                              node?.fetchStatus[LineageDirection.Downstream] === FetchStatus.UNNEEDED)
                    );
                }),
            );
        }
    }, [nodes, direction, prevShownUrns, shownUrns]);

    const { startTimeMillis, endTimeMillis } = useGetLineageTimeParams();
    const { data } = useGetBulkEntityLineageV2Query({
        skip: !urnsToFetch?.length,
        fetchPolicy: 'cache-first',
        variables: {
            urns: urnsToFetch,
            startTimeMillis,
            endTimeMillis,
            separateSiblings: false,
            showColumns: true,
            excludeUpstream: direction !== LineageDirection.Upstream,
            excludeDownstream: direction !== LineageDirection.Downstream,
        },
    });

    const entityRegistry = useEntityRegistryV2();

    const entityDetails = useMemo(
        () =>
            data?.entities?.map<FetchedEntityV2 | null>(
                (entity) =>
                    entity && {
                        ...entityRegistry.getLineageVizConfig(entity.type, entity),
                        lineageAssets: entityRegistry.getLineageAssets(entity.type, entity),
                    },
            ),
        [data, entityRegistry],
    );
    useEffect(() => {
        let changed = false;
        entityDetails?.forEach((entity) => {
            if (!entity) {
                return;
            }
            const node = nodes.get(entity.urn);
            if (node) {
                node.entity = entity;
                changed = true;
            }
        });
        if (changed) {
            setDataVersion((version) => version + 1);
        }
    }, [nodes, direction, entityDetails, entityRegistry, setDataVersion]);
}

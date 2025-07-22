import { useContext, useState } from 'react';

import {
    FetchStatus,
    LineageEntity,
    LineageNodesContext,
    addToAdjacencyList,
    getEdgeId,
    setDefault,
} from '@app/lineageV3/common';

import { useGetDataFlowDagQuery } from '@graphql/dataFlow.generated';
import { Entity, LineageDirection } from '@types';

const DATA_FLOW_CHILD_JOBS_PAGE_SIZE = 50;

export default function useFetchChildDataJobs(): boolean {
    const { rootUrn, nodes, edges, adjacencyList, setNodeVersion, showGhostEntities } = useContext(LineageNodesContext);
    const [start, setStart] = useState(0);
    const [initialized, setInitialized] = useState(false);

    useGetDataFlowDagQuery({
        variables: {
            urn: rootUrn,
            count: DATA_FLOW_CHILD_JOBS_PAGE_SIZE,
            start,
            includeGhostEntities: showGhostEntities,
        },
        onCompleted: (data) => {
            let addedNode = false;

            const jobs = data.dataFlow?.childJobs;
            jobs?.relationships.forEach((job) => {
                if (!job.entity) return;
                addedNode = addedNode || !nodes.has(job.entity.urn);
                const node = setDefault(nodes, job.entity.urn, makeInitialNode(job.entity));
                node.parentDataJob = rootUrn;

                if (job.entity.__typename === 'DataJob') {
                    job.entity.upstream?.relationships.forEach((relationship) => {
                        if (!relationship.entity) return;
                        const edgeId = getEdgeId(node.urn, relationship.entity.urn, LineageDirection.Upstream);
                        edges.set(edgeId, { ...edges.get(edgeId), isDisplayed: true });
                        addToAdjacencyList(adjacencyList, LineageDirection.Upstream, node.urn, relationship.entity.urn);
                    });
                }
            });

            if (jobs?.total && jobs.total > start + DATA_FLOW_CHILD_JOBS_PAGE_SIZE) {
                setStart(start + DATA_FLOW_CHILD_JOBS_PAGE_SIZE);
            }
            setInitialized(true);

            if (addedNode) setNodeVersion((version) => version + 1);
        },
    });

    return initialized;
}

function makeInitialNode({ urn, type }: Entity): LineageEntity {
    return {
        id: urn,
        urn,
        type,
        isExpanded: {
            [LineageDirection.Upstream]: false,
            [LineageDirection.Downstream]: false,
        },
        fetchStatus: {
            [LineageDirection.Upstream]: FetchStatus.UNFETCHED,
            [LineageDirection.Downstream]: FetchStatus.UNFETCHED,
        },
        filters: {
            [LineageDirection.Upstream]: { facetFilters: new Map() },
            [LineageDirection.Downstream]: { facetFilters: new Map() },
        },
    };
}

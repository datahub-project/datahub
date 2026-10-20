import {
    EdgeId,
    FetchStatus,
    LINEAGE_NODE_WIDTH,
    LineageEdge,
    LineageEntity,
    NodeContext,
    getEdgeId,
} from '@app/lineageV3/common';
import { entityNodeDefault } from '@app/lineageV3/queries/useSearchAcrossLineage';
import computeDataFlowGraph from '@app/lineageV3/useComputeGraph/computeDataFlowGraph';
import { getTestEntityRegistry } from '@utils/test-utils/TestPageContainer';

import { EntityType, LineageDirection } from '@types';

const entityRegistry = getTestEntityRegistry();

const ROOT_URN = 'urn:li:dataFlow:(airflow,test-flow,PROD)';
const REPO_URN = 'urn:li:dataset:(urn:li:dataPlatform:git,acme/repo1,PROD)';
const jobUrn = (name: string) => `urn:li:dataJob:(${ROOT_URN},${name})`;
const datasetUrn = (name: string) => `urn:li:dataset:(urn:li:dataPlatform:hive,${name},PROD)`;

function makeRootNode(): LineageEntity {
    return {
        id: ROOT_URN,
        urn: ROOT_URN,
        type: EntityType.DataFlow,
        isExpanded: { [LineageDirection.Upstream]: true, [LineageDirection.Downstream]: true },
        fetchStatus: {
            [LineageDirection.Upstream]: FetchStatus.UNNEEDED,
            [LineageDirection.Downstream]: FetchStatus.UNNEEDED,
        },
        filters: {
            [LineageDirection.Upstream]: { facetFilters: new Map() },
            [LineageDirection.Downstream]: { facetFilters: new Map() },
        },
    };
}

/** An external neighbor as the fetch hooks create it. */
function makeUpstreamNode(urn: string, type = EntityType.Dataset): LineageEntity {
    return entityNodeDefault(urn, type, LineageDirection.Upstream, EntityType.DataFlow);
}

function makeJobNode(urn: string, isExpandedUpstream = false): LineageEntity {
    return {
        ...makeRootNode(),
        id: urn,
        urn,
        type: EntityType.DataJob,
        parentDataJob: ROOT_URN,
        isExpanded: { [LineageDirection.Upstream]: isExpandedUpstream, [LineageDirection.Downstream]: false },
    };
}

function makeGraph(nodeList: LineageEntity[]) {
    const nodes = new Map(nodeList.map((n) => [n.urn, n]));
    const edges = new Map<EdgeId, LineageEdge>();
    const adjacencyList: NodeContext['adjacencyList'] = {
        [LineageDirection.Upstream]: new Map(),
        [LineageDirection.Downstream]: new Map(),
    };
    const addUpstream = (downstream: string, upstream: string) => {
        edges.set(getEdgeId(downstream, upstream, LineageDirection.Upstream), { isDisplayed: true });
        if (!adjacencyList[LineageDirection.Upstream].has(downstream)) {
            adjacencyList[LineageDirection.Upstream].set(downstream, new Set());
        }
        adjacencyList[LineageDirection.Upstream].get(downstream)?.add(upstream);
        if (!adjacencyList[LineageDirection.Downstream].has(upstream)) {
            adjacencyList[LineageDirection.Downstream].set(upstream, new Set());
        }
        adjacencyList[LineageDirection.Downstream].get(upstream)?.add(downstream);
    };
    const compute = () =>
        computeDataFlowGraph(
            ROOT_URN,
            EntityType.DataFlow,
            {
                nodes,
                edges,
                adjacencyList,
                rootType: EntityType.DataFlow,
                hideTransformations: false,
                showDataProcessInstances: true,
                showGhostEntities: true,
            },
            entityRegistry,
            false,
        );
    return { addUpstream, compute };
}

describe('computeDataFlowGraph', () => {
    it("places the DataFlow's own upstream to its left when there are no child jobs", () => {
        const { addUpstream, compute } = makeGraph([makeRootNode(), makeUpstreamNode(REPO_URN)]);
        addUpstream(ROOT_URN, REPO_URN);

        const { flowNodes, flowEdges } = compute();

        const repo = flowNodes.find((n) => n.id === REPO_URN);
        const flow = flowNodes.find((n) => n.id === ROOT_URN);
        expect(repo && flow && repo.position.x < flow.position.x).toBe(true);
        expect(flowEdges.some((e) => e.source === REPO_URN && e.target === ROOT_URN)).toBe(true);
    });

    it('renders nothing with no child jobs and no lineage of its own', () => {
        const { compute } = makeGraph([makeRootNode()]);

        const { flowNodes, flowEdges } = compute();

        expect(flowEdges).toHaveLength(0);
        expect(flowNodes.some((n) => n.id === REPO_URN)).toBe(false);
    });

    it("renders the DataFlow's own upstream outside the box when it also has child jobs", () => {
        const job = jobUrn('job1');
        const { addUpstream, compute } = makeGraph([makeRootNode(), makeJobNode(job), makeUpstreamNode(REPO_URN)]);
        addUpstream(ROOT_URN, REPO_URN);

        const { flowNodes, flowEdges } = compute();

        expect(flowNodes.some((n) => n.id === job)).toBe(true);
        expect(flowNodes.find((n) => n.id === REPO_URN)?.position.x).toBeLessThan(0);
        expect(flowEdges.some((e) => e.source === REPO_URN && e.target === ROOT_URN)).toBe(true);
    });

    it("keeps the DataFlow's own upstream clear of a job's upstream column", () => {
        const jobs = [jobUrn('a'), jobUrn('b'), jobUrn('c')];
        const jobUpstream = datasetUrn('b_input');
        const { addUpstream, compute } = makeGraph([
            makeRootNode(),
            makeJobNode(jobs[0]),
            makeJobNode(jobs[1], true),
            makeJobNode(jobs[2]),
            makeUpstreamNode(jobUpstream),
            makeUpstreamNode(REPO_URN),
        ]);
        addUpstream(jobs[1], jobUpstream);
        addUpstream(ROOT_URN, REPO_URN);

        const { flowNodes } = compute();

        const repo = flowNodes.find((n) => n.id === REPO_URN);
        const input = flowNodes.find((n) => n.id === jobUpstream);
        expect(repo && input).toBeTruthy();
        expect((repo?.position.x ?? 0) + LINEAGE_NODE_WIDTH).toBeLessThanOrEqual(input?.position.x ?? 0);
    });

    it('renders a repository linked to both the DataFlow and a child job once, with both edges', () => {
        const job = jobUrn('job1');
        const { addUpstream, compute } = makeGraph([
            makeRootNode(),
            makeJobNode(job, true),
            makeUpstreamNode(REPO_URN),
        ]);
        addUpstream(ROOT_URN, REPO_URN);
        addUpstream(job, REPO_URN);

        const { flowNodes, flowEdges } = compute();

        expect(flowNodes.filter((n) => n.id === REPO_URN)).toHaveLength(1);
        expect(flowEdges.some((e) => e.source === REPO_URN && e.target === ROOT_URN)).toBe(true);
        expect(flowEdges.some((e) => e.source === REPO_URN && e.target === job)).toBe(true);
    });
});

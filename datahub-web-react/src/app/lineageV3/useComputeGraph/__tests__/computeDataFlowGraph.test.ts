import {
    DATA_FLOW_LINEAGE_CONTROL_NODE_NAME,
    DataFlowLineageControlData,
} from '@app/lineageV3/LineageBoundingBoxNode/DataFlowLineageControlNode';
import {
    BOUNDING_BOX_HANDLE_Y,
    LINEAGE_BOUNDING_BOX_NODE_NAME,
} from '@app/lineageV3/LineageBoundingBoxNode/LineageBoundingBoxNode';
import {
    EdgeId,
    FetchStatus,
    LINEAGE_FILTER_PAGINATION,
    LINEAGE_HANDLE_OFFSET,
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
const DATA_FLOW_NODE_HEIGHT = 80;

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
            [LineageDirection.Upstream]: FetchStatus.COMPLETE,
            [LineageDirection.Downstream]: FetchStatus.COMPLETE,
        },
        filters: {
            [LineageDirection.Upstream]: { limit: LINEAGE_FILTER_PAGINATION, facetFilters: new Map() },
            [LineageDirection.Downstream]: { limit: LINEAGE_FILTER_PAGINATION, facetFilters: new Map() },
        },
    };
}

/** An external neighbor as the fetch hooks create it. */
function makeUpstreamNode(urn: string, type = EntityType.Dataset): LineageEntity {
    return entityNodeDefault(urn, type, LineageDirection.Upstream, EntityType.DataFlow);
}

function makeDownstreamNode(urn: string, type = EntityType.Dataset): LineageEntity {
    return entityNodeDefault(urn, type, LineageDirection.Downstream, EntityType.DataFlow);
}

function expectNoOverlaps(flowNodes: { id: string; type?: string; position: { x: number; y: number } }[]) {
    const boxes = flowNodes.filter((n) => n.id !== ROOT_URN && n.type !== DATA_FLOW_LINEAGE_CONTROL_NODE_NAME);
    boxes.forEach((a, i) =>
        boxes.slice(i + 1).forEach((b) => {
            const overlaps =
                Math.abs(a.position.x - b.position.x) < LINEAGE_NODE_WIDTH &&
                Math.abs(a.position.y - b.position.y) < DATA_FLOW_NODE_HEIGHT;
            expect(overlaps ? `${a.id} overlaps ${b.id}` : undefined).toBeUndefined();
        }),
    );
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
    const addNode = (node: LineageEntity) => nodes.set(node.urn, node);
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
    return { addNode, addUpstream, compute };
}

describe('computeDataFlowGraph', () => {
    it("places the DataFlow's own upstream to its left when there are no child jobs", () => {
        const { addUpstream, compute } = makeGraph([makeRootNode(), makeUpstreamNode(REPO_URN)]);
        addUpstream(ROOT_URN, REPO_URN);

        const { flowNodes, flowEdges } = compute();

        const repo = flowNodes.find((n) => n.id === REPO_URN);
        const flow = flowNodes.find((n) => n.id === ROOT_URN);
        expect(flow?.type).toBe(LINEAGE_BOUNDING_BOX_NODE_NAME);
        expect((repo?.position.y ?? 0) + LINEAGE_HANDLE_OFFSET).toBe(BOUNDING_BOX_HANDLE_Y);
        const controls = flowNodes.filter((n) => n.type === DATA_FLOW_LINEAGE_CONTROL_NODE_NAME);
        expect(controls.map((n) => [n.parentId, (n.data as DataFlowLineageControlData).direction])).toEqual([
            [ROOT_URN, LineageDirection.Upstream],
        ]);
        expect((repo?.position.x ?? 0) + LINEAGE_NODE_WIDTH).toBeLessThan(flow?.position.x ?? -Infinity);
        expect(flowEdges.some((e) => e.source === REPO_URN && e.target === ROOT_URN)).toBe(true);
    });

    it('renders just an empty box with no child jobs and no lineage of its own', () => {
        const { compute } = makeGraph([makeRootNode()]);

        const { flowNodes, flowEdges } = compute();

        expect(flowNodes.map((n) => n.type)).toEqual([LINEAGE_BOUNDING_BOX_NODE_NAME]);
        expect(flowEdges).toHaveLength(0);
    });

    it("keeps the DataFlow's own upstream level with the title when job lineage leaves room there", () => {
        const jobs = [jobUrn('a'), jobUrn('b'), jobUrn('c')];
        const jobUpstream = datasetUrn('bottom_input');
        const jobNodes = jobs.map((job) => makeJobNode(job));
        const { addUpstream, compute } = makeGraph([
            makeRootNode(),
            ...jobNodes,
            makeUpstreamNode(jobUpstream),
            makeUpstreamNode(REPO_URN),
        ]);
        addUpstream(ROOT_URN, REPO_URN);
        const jobYs = compute().flowNodes.filter((n) => jobs.includes(n.id));
        const bottomJob = jobYs.reduce((a, b) => (b.position.y > a.position.y ? b : a)).id;
        addUpstream(bottomJob, jobUpstream);
        const bottom = jobNodes.find((n) => n.urn === bottomJob);
        if (bottom) bottom.isExpanded[LineageDirection.Upstream] = true;

        const { flowNodes } = compute();

        const repo = flowNodes.find((n) => n.id === REPO_URN);
        expect((repo?.position.y ?? 0) + LINEAGE_HANDLE_OFFSET).toBe(BOUNDING_BOX_HANDLE_Y);
        expect(flowNodes.some((n) => n.id === jobUpstream)).toBe(true);
        expectNoOverlaps(flowNodes);
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

    it("stacks the DataFlow's own upstream above a job's upstream, in the same column", () => {
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
        expect(repo?.position.x).toBe(input?.position.x);
        expect(repo?.position.y).toBeLessThan(input?.position.y ?? -Infinity);
        expectNoOverlaps(flowNodes);
    });

    it("places the DataFlow's own downstream right of the box, stacked above a job's downstream", () => {
        const job = jobUrn('job1');
        const jobNode = makeJobNode(job);
        jobNode.isExpanded = { [LineageDirection.Upstream]: false, [LineageDirection.Downstream]: true };
        const jobOutput = datasetUrn('job_output');
        const flowOutput = datasetUrn('flow_output');
        const { addUpstream, compute } = makeGraph([
            makeRootNode(),
            jobNode,
            makeDownstreamNode(jobOutput),
            makeDownstreamNode(flowOutput),
        ]);
        addUpstream(jobOutput, job);
        addUpstream(flowOutput, ROOT_URN);

        const { flowNodes, flowEdges } = compute();

        const box = flowNodes.find((n) => n.id === ROOT_URN);
        const output = flowNodes.find((n) => n.id === flowOutput);
        const jobOut = flowNodes.find((n) => n.id === jobOutput);
        expect(output?.position.x).toBeGreaterThanOrEqual(box?.width ?? Infinity);
        expect(output?.position.x).toBe(jobOut?.position.x);
        expect(output?.position.y).toBeLessThan(jobOut?.position.y ?? -Infinity);
        expect(flowEdges.some((e) => e.source === ROOT_URN && e.target === flowOutput)).toBe(true);
        expectNoOverlaps(flowNodes);
    });

    it('paginates many direct neighbors on both sides without overlapping job lineage', () => {
        const job = jobUrn('job1');
        const jobNode = makeJobNode(job, true);
        jobNode.isExpanded[LineageDirection.Downstream] = true;
        const { addNode, addUpstream, compute } = makeGraph([makeRootNode(), jobNode]);
        const count = LINEAGE_FILTER_PAGINATION + 3;
        Array.from({ length: count }).forEach((_, i) => {
            addNode(makeUpstreamNode(datasetUrn(`flow_in_${i}`)));
            addUpstream(ROOT_URN, datasetUrn(`flow_in_${i}`));
            addNode(makeDownstreamNode(datasetUrn(`flow_out_${i}`)));
            addUpstream(datasetUrn(`flow_out_${i}`), ROOT_URN);
            addNode(makeUpstreamNode(datasetUrn(`job_in_${i}`)));
            addUpstream(job, datasetUrn(`job_in_${i}`));
            addNode(makeDownstreamNode(datasetUrn(`job_out_${i}`)));
            addUpstream(datasetUrn(`job_out_${i}`), job);
        });

        const { flowNodes, lineageFilters } = compute();

        expect(lineageFilters.size).toBeGreaterThanOrEqual(4);
        const shown = (prefix: string) => flowNodes.filter((n) => n.id.includes(`,${prefix}_`)).length;
        expect(shown('flow_in')).toBe(LINEAGE_FILTER_PAGINATION);
        expect(shown('flow_out')).toBe(LINEAGE_FILTER_PAGINATION);
        expectNoOverlaps(flowNodes);
    });

    it("stacks the DataFlow's multi-hop lineage above the jobs' multi-hop lineage, column by column", () => {
        const job = jobUrn('job1');
        const jobIn = makeUpstreamNode(datasetUrn('job_in'));
        jobIn.isExpanded[LineageDirection.Upstream] = true;
        const repo = makeUpstreamNode(REPO_URN);
        repo.isExpanded[LineageDirection.Upstream] = true;
        const { addUpstream, compute } = makeGraph([
            makeRootNode(),
            makeJobNode(job, true),
            jobIn,
            makeUpstreamNode(datasetUrn('job_in_2')),
            makeUpstreamNode(datasetUrn('job_in_3')),
            repo,
            makeUpstreamNode(datasetUrn('repo_in')),
            makeUpstreamNode(datasetUrn('repo_in_other')),
        ]);
        addUpstream(job, datasetUrn('job_in'));
        addUpstream(datasetUrn('job_in'), datasetUrn('job_in_2'));
        addUpstream(datasetUrn('job_in'), datasetUrn('job_in_3'));
        addUpstream(ROOT_URN, REPO_URN);
        addUpstream(REPO_URN, datasetUrn('repo_in'));
        addUpstream(REPO_URN, datasetUrn('repo_in_other'));

        const { flowNodes, flowEdges } = compute();

        const at = (urn: string) => flowNodes.find((n) => n.id === urn)?.position;
        expect(at(REPO_URN)?.x).toBe(at(datasetUrn('job_in'))?.x);
        expect(at(datasetUrn('repo_in'))?.x).toBe(at(datasetUrn('job_in_2'))?.x);
        const highestJobNeighbor = Math.min(
            ...['job_in', 'job_in_2', 'job_in_3'].map((name) => at(datasetUrn(name))?.y ?? -Infinity),
        );
        ['repo_in', 'repo_in_other'].forEach((name) =>
            expect(at(datasetUrn(name))?.y).toBeLessThan(highestJobNeighbor),
        );
        expect(flowEdges.some((e) => e.source === datasetUrn('repo_in') && e.target === REPO_URN)).toBe(true);
        expectNoOverlaps(flowNodes);
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

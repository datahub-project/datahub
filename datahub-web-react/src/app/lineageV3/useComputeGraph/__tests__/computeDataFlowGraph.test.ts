import { EdgeId, FetchStatus, LineageEdge, LineageEntity, NodeContext, getEdgeId } from '@app/lineageV3/common';
import computeDataFlowGraph from '@app/lineageV3/useComputeGraph/computeDataFlowGraph';

import { EntityType, LineageDirection } from '@types';

// Regression coverage: a DataFlow's own direct upstream/downstream
// (e.g. a linked source-code Repository via repositoryLineage) must render regardless of whether
// the DataFlow also has child DataJobs. Before this fix, the DataFlow's own edge was silently
// dropped by the child-job filter in `hideNodes`, and there was no code path that ever considered
// the root DataFlow itself (as opposed to its child jobs) as having its own external lineage.
describe('computeDataFlowGraph', () => {
    const ROOT_URN = 'urn:li:dataFlow:(airflow,test-flow,PROD)';
    const REPO_URN = 'urn:li:dataset:(urn:li:dataPlatform:git,acme/repo1,PROD)';
    const JOB_URN = 'urn:li:dataJob:(urn:li:dataFlow:(airflow,test-flow,PROD),job1)';

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

    function makeLeafNode(urn: string, type: EntityType, isExpanded = false): LineageEntity {
        return {
            id: urn,
            urn,
            type,
            isExpanded: { [LineageDirection.Upstream]: isExpanded, [LineageDirection.Downstream]: isExpanded },
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

    function makeJobNode(urn: string, isExpanded = false): LineageEntity {
        return { ...makeLeafNode(urn, EntityType.DataJob, isExpanded), parentDataJob: ROOT_URN };
    }

    function makeContext(
        nodes: Map<string, LineageEntity>,
        edges: Map<EdgeId, LineageEdge>,
        adjacencyList: NodeContext['adjacencyList'],
    ): Pick<
        NodeContext,
        | 'nodes'
        | 'edges'
        | 'adjacencyList'
        | 'rootType'
        | 'hideTransformations'
        | 'showDataProcessInstances'
        | 'showGhostEntities'
    > {
        return {
            nodes,
            edges,
            adjacencyList,
            rootType: EntityType.DataFlow,
            hideTransformations: false,
            showDataProcessInstances: true,
            showGhostEntities: true,
        };
    }

    function addUpstreamEdge(
        nodes: Map<string, LineageEntity>,
        edges: Map<EdgeId, LineageEdge>,
        adjacencyList: NodeContext['adjacencyList'],
        downstream: string,
        upstream: string,
    ) {
        edges.set(getEdgeId(downstream, upstream, LineageDirection.Upstream), { isDisplayed: true });
        adjacencyList[LineageDirection.Upstream].set(downstream, new Set([upstream]));
        adjacencyList[LineageDirection.Downstream].set(upstream, new Set([downstream]));
    }

    it("renders the DataFlow's own direct upstream when there are no child jobs", () => {
        const nodes = new Map([
            [ROOT_URN, makeRootNode()],
            [REPO_URN, makeLeafNode(REPO_URN, EntityType.Dataset)],
        ]);
        const edges = new Map<EdgeId, LineageEdge>();
        const adjacencyList = {
            [LineageDirection.Upstream]: new Map<string, Set<string>>(),
            [LineageDirection.Downstream]: new Map<string, Set<string>>(),
        };
        addUpstreamEdge(nodes, edges, adjacencyList, ROOT_URN, REPO_URN);

        const { flowNodes, flowEdges } = computeDataFlowGraph(
            ROOT_URN,
            EntityType.DataFlow,
            makeContext(nodes, edges, adjacencyList),
            false,
        );

        expect(flowNodes.some((n) => n.id === REPO_URN)).toBe(true);
        expect(flowEdges.some((e) => e.source === REPO_URN && e.target === ROOT_URN)).toBe(true);
    });

    it('renders nothing but does not crash with no child jobs and no direct lineage', () => {
        const nodes = new Map([[ROOT_URN, makeRootNode()]]);
        const edges = new Map<EdgeId, LineageEdge>();
        const adjacencyList = {
            [LineageDirection.Upstream]: new Map<string, Set<string>>(),
            [LineageDirection.Downstream]: new Map<string, Set<string>>(),
        };

        const { flowNodes, flowEdges } = computeDataFlowGraph(
            ROOT_URN,
            EntityType.DataFlow,
            makeContext(nodes, edges, adjacencyList),
            false,
        );

        expect(flowEdges.length).toBe(0);
        expect(flowNodes.every((n) => n.id !== REPO_URN)).toBe(true);
    });

    // The reported case: the DataFlow has a
    // repository linked directly to it, and it has a child DataJob, but that DataJob carries no
    // repository edge of its own. The repository must still render, attributed to the DataFlow.
    it("renders the DataFlow's own direct upstream when it also has a child job with no lineage of its own", () => {
        const nodes = new Map([
            [ROOT_URN, makeRootNode()],
            [JOB_URN, makeJobNode(JOB_URN)],
            [REPO_URN, makeLeafNode(REPO_URN, EntityType.Dataset)],
        ]);
        const edges = new Map<EdgeId, LineageEdge>();
        const adjacencyList = {
            [LineageDirection.Upstream]: new Map<string, Set<string>>(),
            [LineageDirection.Downstream]: new Map<string, Set<string>>(),
        };
        addUpstreamEdge(nodes, edges, adjacencyList, ROOT_URN, REPO_URN);

        const { flowNodes, flowEdges } = computeDataFlowGraph(
            ROOT_URN,
            EntityType.DataFlow,
            makeContext(nodes, edges, adjacencyList),
            false,
        );

        expect(flowNodes.some((n) => n.id === JOB_URN)).toBe(true);
        expect(flowNodes.some((n) => n.id === REPO_URN)).toBe(true);
        expect(flowEdges.some((e) => e.source === REPO_URN && e.target === ROOT_URN)).toBe(true);

        // The repository must render outside the bounding box (which spans x >= 0), not overlap
        // or nest inside it looking like one of the DataFlow's own tasks.
        const repoNode = flowNodes.find((n) => n.id === REPO_URN);
        expect(repoNode?.position.x).toBeLessThan(0);
    });

    it("still renders a child job's own upstream lineage when it has one, unaffected by the DataFlow having none", () => {
        const upstreamDatasetUrn = 'urn:li:dataset:(urn:li:dataPlatform:hive,upstream1,PROD)';
        const nodes = new Map([
            [ROOT_URN, makeRootNode()],
            [JOB_URN, makeJobNode(JOB_URN, true)],
            [upstreamDatasetUrn, makeLeafNode(upstreamDatasetUrn, EntityType.Dataset)],
        ]);
        const edges = new Map<EdgeId, LineageEdge>();
        const adjacencyList = {
            [LineageDirection.Upstream]: new Map<string, Set<string>>(),
            [LineageDirection.Downstream]: new Map<string, Set<string>>(),
        };
        addUpstreamEdge(nodes, edges, adjacencyList, JOB_URN, upstreamDatasetUrn);

        const { flowNodes } = computeDataFlowGraph(
            ROOT_URN,
            EntityType.DataFlow,
            makeContext(nodes, edges, adjacencyList),
            false,
        );

        // Edge attribution for a job's own external lineage depends on traversal-assigned node
        // `.direction`, which a hand-built mock doesn't replicate -- not something this fix changes,
        // so this only asserts the node itself still renders, unaffected by the DataFlow's own fix.
        expect(flowNodes.some((n) => n.id === JOB_URN)).toBe(true);
        expect(flowNodes.some((n) => n.id === upstreamDatasetUrn)).toBe(true);
    });

    // The same upstream linked to both the DataFlow itself and to one of its child jobs. Both edges are real and should render, but the repo
    // node itself must not be duplicated.
    it('dedupes when both the DataFlow and a child job link to the same repository', () => {
        const nodes = new Map([
            [ROOT_URN, makeRootNode()],
            [JOB_URN, makeJobNode(JOB_URN, true)],
            [REPO_URN, makeLeafNode(REPO_URN, EntityType.Dataset)],
        ]);
        const edges = new Map<EdgeId, LineageEdge>();
        const adjacencyList = {
            [LineageDirection.Upstream]: new Map<string, Set<string>>(),
            [LineageDirection.Downstream]: new Map<string, Set<string>>(),
        };
        addUpstreamEdge(nodes, edges, adjacencyList, ROOT_URN, REPO_URN);
        adjacencyList[LineageDirection.Upstream].get(ROOT_URN)?.add(REPO_URN);
        // Add the job's own edge to the same repo, on top of the DataFlow's edge above.
        adjacencyList[LineageDirection.Upstream].set(
            JOB_URN,
            new Set([...(adjacencyList[LineageDirection.Upstream].get(JOB_URN) ?? []), REPO_URN]),
        );
        adjacencyList[LineageDirection.Downstream].set(
            REPO_URN,
            new Set([...(adjacencyList[LineageDirection.Downstream].get(REPO_URN) ?? []), JOB_URN]),
        );
        edges.set(getEdgeId(JOB_URN, REPO_URN, LineageDirection.Upstream), { isDisplayed: true });

        const { flowNodes, flowEdges } = computeDataFlowGraph(
            ROOT_URN,
            EntityType.DataFlow,
            makeContext(nodes, edges, adjacencyList),
            false,
        );

        expect(flowNodes.filter((n) => n.id === REPO_URN).length).toBe(1);
        // Both links are real and must both be visible: the DataFlow's own edge to the box, and
        // the job's own edge to the same repo. Previously the job's edge silently disappeared
        // (extraEdgeData depended on `.direction`, which is only set for cyclic nodes), making it
        // look like the repo was linked to only one of the two.
        expect(flowEdges.some((e) => e.source === REPO_URN && e.target === ROOT_URN)).toBe(true);
        expect(flowEdges.some((e) => e.source === REPO_URN && e.target === JOB_URN)).toBe(true);
    });
});

/* eslint-disable import/first, @typescript-eslint/no-non-null-assertion */
// `vi.mock` calls are hoisted above all imports by vitest, but eslint's `import/first` rule
// doesn't know that. The non-null assertions are intentional: the test asserts the element is
// present immediately before each access.
// Stub the visualisation modules so vitest's eager file collection doesn't evaluate the
// LineageVisualization -> LineageTransformationNode -> NodeWrapper chain, which fails in
// jsdom because `styled(NodeWrapper)` is evaluated before NodeWrapper finishes its own
// module initialisation. The tests below exercise pure layout logic and don't need any
// visualisation components.
import { vi } from 'vitest';

import {
    BOUNDING_BOX_PADDING,
    LINEAGE_BOUNDING_BOX_NODE_NAME,
} from '@app/lineageV3/LineageBoundingBoxNode/LineageBoundingBoxNode';
import { FetchStatus, LineageBoundingBox, LineageEntity } from '@app/lineageV3/common';
import { LineageVisualizationNode } from '@app/lineageV3/useComputeGraph/NodeBuilder';
import { addNeighborDomainBoxes } from '@app/lineageV3/useComputeGraph/computeDomainGraph';

import { EntityType, LineageDirection } from '@types';

vi.mock('@app/lineageV3/LineageVisualization', () => ({}));
vi.mock('@app/lineageV3/LineageTransformationNode/LineageTransformationNode', () => ({
    LINEAGE_TRANSFORMATION_NODE_NAME: 'lineage-transformation',
    TRANSFORMATION_NODE_SIZE: 40,
}));

function makeNode(urn: string, type: EntityType, extras: Partial<LineageEntity> = {}, entity?: any): LineageEntity {
    return {
        id: urn,
        urn,
        type,
        entity,
        isExpanded: { [LineageDirection.Upstream]: true, [LineageDirection.Downstream]: true },
        fetchStatus: {
            [LineageDirection.Upstream]: FetchStatus.UNFETCHED,
            [LineageDirection.Downstream]: FetchStatus.UNFETCHED,
        },
        filters: {
            [LineageDirection.Upstream]: { facetFilters: new Map() },
            [LineageDirection.Downstream]: { facetFilters: new Map() },
        },
        ...extras,
    };
}

function makeRootBoundingBox() {
    return {
        id: 'urn:li:domain:root',
        type: LINEAGE_BOUNDING_BOX_NODE_NAME,
        position: { x: 0, y: 0 },
        data: {
            urn: 'urn:li:domain:root',
            type: EntityType.Domain,
        } as LineageBoundingBox,
        width: 400,
        height: 300,
        style: { width: 400, height: 300 },
    };
}

describe('addNeighborDomainBoxes', () => {
    const rootUrn = 'urn:li:domain:root';
    const upstreamDomainUrn = 'urn:li:domain:upstream';
    const downstreamDomainUrn = 'urn:li:domain:downstream';

    it('creates a single bbox per declared neighbour Domain', () => {
        // Two declared neighbours via domainUpstreams: one upstream, one downstream. The bboxes
        // must be keyed by neighbour URN and sit on the correct side of the root box.
        const nodes = new Map<string, LineageEntity>([
            [rootUrn, makeNode(rootUrn, EntityType.Domain)],
            [upstreamDomainUrn, makeNode(upstreamDomainUrn, EntityType.Domain)],
            [downstreamDomainUrn, makeNode(downstreamDomainUrn, EntityType.Domain)],
        ]);

        const adjacencyList = {
            [LineageDirection.Upstream]: new Map([[rootUrn, new Set([upstreamDomainUrn])]]),
            [LineageDirection.Downstream]: new Map([[rootUrn, new Set([downstreamDomainUrn])]]),
        };

        const flowNodes: LineageVisualizationNode[] = [];
        const root = makeRootBoundingBox();
        addNeighborDomainBoxes(flowNodes, nodes, adjacencyList, root as any, rootUrn);

        const bboxes = flowNodes.filter((n) => n.type === LINEAGE_BOUNDING_BOX_NODE_NAME);
        expect(bboxes.map((b) => b.id).sort()).toEqual([downstreamDomainUrn, upstreamDomainUrn]);
    });

    it('places upstream neighbours to the left and downstream neighbours to the right of root', () => {
        const nodes = new Map<string, LineageEntity>([
            [rootUrn, makeNode(rootUrn, EntityType.Domain)],
            [upstreamDomainUrn, makeNode(upstreamDomainUrn, EntityType.Domain)],
            [downstreamDomainUrn, makeNode(downstreamDomainUrn, EntityType.Domain)],
        ]);
        const adjacencyList = {
            [LineageDirection.Upstream]: new Map([[rootUrn, new Set([upstreamDomainUrn])]]),
            [LineageDirection.Downstream]: new Map([[rootUrn, new Set([downstreamDomainUrn])]]),
        };

        const flowNodes: LineageVisualizationNode[] = [];
        const root = makeRootBoundingBox();
        addNeighborDomainBoxes(flowNodes, nodes, adjacencyList, root as any, rootUrn);

        const upstream = flowNodes.find((n) => n.id === upstreamDomainUrn);
        const downstream = flowNodes.find((n) => n.id === downstreamDomainUrn);
        expect(upstream).toBeTruthy();
        expect(downstream).toBeTruthy();

        // Upstream sits strictly to the left of the root box; downstream strictly to the right.
        expect(upstream!.position.x).toBeLessThan(root.position.x);
        expect(downstream!.position.x).toBeGreaterThan(root.position.x + root.width);

        // Each neighbour bbox is at least sized for a single entity row.
        expect(upstream!.width!).toBeGreaterThanOrEqual(220 - BOUNDING_BOX_PADDING);
        expect(downstream!.width!).toBeGreaterThanOrEqual(220 - BOUNDING_BOX_PADDING);
    });

    it('filters out non-Domain neighbours that landed in the adjacency list', () => {
        // searchAcrossLineage on a Domain URN can in principle surface non-Domain entities (e.g.
        // datasets that have an `entityDomain` relationship). The neighbour layout must only draw
        // boxes for actual Domain entities so the Domain lineage tab stays focused on the Domain
        // layer.
        const datasetUrn = 'urn:li:dataset:noise';
        const nodes = new Map<string, LineageEntity>([
            [rootUrn, makeNode(rootUrn, EntityType.Domain)],
            [upstreamDomainUrn, makeNode(upstreamDomainUrn, EntityType.Domain)],
            [datasetUrn, makeNode(datasetUrn, EntityType.Dataset)],
        ]);
        const adjacencyList = {
            [LineageDirection.Upstream]: new Map([[rootUrn, new Set([upstreamDomainUrn, datasetUrn])]]),
            [LineageDirection.Downstream]: new Map<string, Set<string>>(),
        };

        const flowNodes: LineageVisualizationNode[] = [];
        addNeighborDomainBoxes(flowNodes, nodes, adjacencyList, makeRootBoundingBox() as any, rootUrn);

        const bboxes = flowNodes.filter((n) => n.type === LINEAGE_BOUNDING_BOX_NODE_NAME);
        expect(bboxes.map((b) => b.id)).toEqual([upstreamDomainUrn]);
    });

    it('ignores self-edges (Domain pointing at itself)', () => {
        // The backend validator already rejects self-edges, but the renderer must not draw a
        // self-bbox even if a stale or test fixture sneaks one through. This is a cheap belt-and-
        // suspenders check.
        const nodes = new Map<string, LineageEntity>([[rootUrn, makeNode(rootUrn, EntityType.Domain)]]);
        const adjacencyList = {
            [LineageDirection.Upstream]: new Map([[rootUrn, new Set([rootUrn])]]),
            [LineageDirection.Downstream]: new Map<string, Set<string>>(),
        };

        const flowNodes: LineageVisualizationNode[] = [];
        addNeighborDomainBoxes(flowNodes, nodes, adjacencyList, makeRootBoundingBox() as any, rootUrn);
        expect(flowNodes).toEqual([]);
    });

    it('produces no neighbour boxes when the adjacency list is empty', () => {
        const nodes = new Map<string, LineageEntity>([[rootUrn, makeNode(rootUrn, EntityType.Domain)]]);
        const adjacencyList = {
            [LineageDirection.Upstream]: new Map<string, Set<string>>(),
            [LineageDirection.Downstream]: new Map<string, Set<string>>(),
        };

        const flowNodes: LineageVisualizationNode[] = [];
        addNeighborDomainBoxes(flowNodes, nodes, adjacencyList, makeRootBoundingBox() as any, rootUrn);
        expect(flowNodes).toEqual([]);
    });
});

import { vi } from 'vitest';

import {
    BOUNDING_BOX_PADDING,
    LINEAGE_BOUNDING_BOX_NODE_NAME,
} from '@app/lineageV3/LineageBoundingBoxNode/LineageBoundingBoxNode';
import { LINEAGE_ENTITY_NODE_NAME } from '@app/lineageV3/LineageEntityNode/LineageEntityNode';
import { FetchStatus, LineageBoundingBox, LineageEntity } from '@app/lineageV3/common';
import { LineageVisualizationNode } from '@app/lineageV3/useComputeGraph/NodeBuilder';
import { addNeighborDataProductBoxes } from '@app/lineageV3/useComputeGraph/computeDataProductGraph';

import { EntityType, LineageDirection } from '@types';

// Stub the visualisation modules so vitest's eager file collection doesn't evaluate the
// LineageVisualization -> LineageTransformationNode -> NodeWrapper chain, which fails in
// jsdom because `styled(NodeWrapper)` is evaluated before NodeWrapper finishes its own
// module initialisation. Vitest hoists `vi.mock` to the top of the file at runtime so the
// stubs apply before the imports above resolve. The tests below exercise pure layout logic
// and don't need any visualisation components.
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

function makeFlowNode(id: string, x: number, y: number): LineageVisualizationNode {
    return {
        id,
        type: LINEAGE_ENTITY_NODE_NAME,
        position: { x, y },
        data: { urn: id, type: EntityType.Dataset } as any,
    } as LineageVisualizationNode;
}

function makeRootBoundingBox(): {
    id: string;
    type: string;
    position: { x: number; y: number };
    data: LineageBoundingBox;
    width: number;
    height: number;
    style: { width: number; height: number };
} {
    return {
        id: 'root',
        type: LINEAGE_BOUNDING_BOX_NODE_NAME,
        position: { x: 0, y: 0 },
        data: {
            urn: 'urn:li:dataProduct:root',
            type: EntityType.DataProduct,
        } as LineageBoundingBox,
        width: 400,
        height: 300,
        style: { width: 400, height: 300 },
    };
}

describe('addNeighborDataProductBoxes', () => {
    const rootUrn = 'urn:li:dataProduct:root';
    const neighbourDpUrn = 'urn:li:dataProduct:other';

    it('creates a single bbox per declared-only neighbour DP', () => {
        const nodes = new Map<string, LineageEntity>([
            [rootUrn, makeNode(rootUrn, EntityType.DataProduct)],
            [
                neighbourDpUrn,
                makeNode(
                    neighbourDpUrn,
                    EntityType.DataProduct,
                    {},
                    {
                        name: 'Other DP',
                        genericEntityProperties: {
                            type: EntityType.DataProduct,
                            properties: { name: 'Other DP' },
                        } as any,
                    },
                ),
            ],
        ]);

        const adjacencyList = {
            [LineageDirection.Upstream]: new Map<string, Set<string>>(),
            [LineageDirection.Downstream]: new Map([[rootUrn, new Set([neighbourDpUrn])]]),
        };

        const flowNodes: LineageVisualizationNode[] = [];
        addNeighborDataProductBoxes(flowNodes, nodes, adjacencyList, new Set(), rootUrn, makeRootBoundingBox() as any);

        const bboxes = flowNodes.filter((n) => n.type === LINEAGE_BOUNDING_BOX_NODE_NAME);
        expect(bboxes).toHaveLength(1);
        expect(bboxes[0].id).toBe(neighbourDpUrn);
    });

    it('merges a declared neighbour DP with an inferred neighbour DP into one bbox', () => {
        const neighbourAssetUrn = 'urn:li:dataset:asset-in-other-dp';
        const memberUrn = 'urn:li:dataset:my-member';
        const memberUrns = new Set([memberUrn]);

        // Neighbour asset carries entityDataProduct -> Other DP (inferred relationship).
        const neighbourAssetEntity = {
            genericEntityProperties: {
                dataProduct: {
                    relationships: [
                        {
                            entity: {
                                urn: neighbourDpUrn,
                                type: EntityType.DataProduct,
                                properties: { name: 'Other DP' },
                            },
                        },
                    ],
                },
            },
        };

        const nodes = new Map<string, LineageEntity>([
            [rootUrn, makeNode(rootUrn, EntityType.DataProduct)],
            [memberUrn, makeNode(memberUrn, EntityType.Dataset, { parentDataProduct: rootUrn })],
            [neighbourAssetUrn, makeNode(neighbourAssetUrn, EntityType.Dataset, {}, neighbourAssetEntity)],
            [
                neighbourDpUrn,
                makeNode(
                    neighbourDpUrn,
                    EntityType.DataProduct,
                    {},
                    {
                        name: 'Other DP',
                        genericEntityProperties: {
                            type: EntityType.DataProduct,
                            properties: { name: 'Other DP' },
                        } as any,
                    },
                ),
            ],
        ]);

        const adjacencyList = {
            [LineageDirection.Upstream]: new Map<string, Set<string>>(),
            // Both declared (root -> otherDp) and inferred (member -> neighbourAsset whose DP is otherDp).
            [LineageDirection.Downstream]: new Map([
                [rootUrn, new Set([neighbourDpUrn])],
                [memberUrn, new Set([neighbourAssetUrn])],
            ]),
        };

        const flowNodes: LineageVisualizationNode[] = [makeFlowNode(neighbourAssetUrn, 600, 50)];

        addNeighborDataProductBoxes(flowNodes, nodes, adjacencyList, memberUrns, rootUrn, makeRootBoundingBox() as any);

        const bboxes = flowNodes.filter((n) => n.type === LINEAGE_BOUNDING_BOX_NODE_NAME);
        // A single bbox keyed by the neighbour DP urn — declared and inferred lineage
        // collapse into one visual line per neighbour DP, matching the table / column
        // lineage convention.
        expect(bboxes).toHaveLength(1);
        expect(bboxes[0].id).toBe(neighbourDpUrn);

        // The neighbour asset has been adopted as a child of the bbox (parentId === neighbour DP).
        const neighbourAsset = flowNodes.find((n) => n.id === neighbourAssetUrn);
        expect(neighbourAsset?.parentId).toBe(neighbourDpUrn);
    });

    it('positions a declared-only placeholder box outside the root box', () => {
        const root = makeRootBoundingBox();
        const nodes = new Map<string, LineageEntity>([
            [rootUrn, makeNode(rootUrn, EntityType.DataProduct)],
            [
                neighbourDpUrn,
                makeNode(
                    neighbourDpUrn,
                    EntityType.DataProduct,
                    {},
                    {
                        name: 'Other DP',
                        genericEntityProperties: {
                            type: EntityType.DataProduct,
                            properties: { name: 'Other DP' },
                        } as any,
                    },
                ),
            ],
        ]);
        const adjacencyList = {
            [LineageDirection.Upstream]: new Map<string, Set<string>>(),
            [LineageDirection.Downstream]: new Map([[rootUrn, new Set([neighbourDpUrn])]]),
        };

        const flowNodes: LineageVisualizationNode[] = [];
        addNeighborDataProductBoxes(flowNodes, nodes, adjacencyList, new Set(), rootUrn, root as any);

        const bbox = flowNodes.find((n) => n.id === neighbourDpUrn);
        expect(bbox).toBeTruthy();
        // Downstream neighbour should sit to the right of the root box.
        expect(bbox!.position.x).toBeGreaterThan(root.position.x + root.width);
        // And occupy at least minimum sensible dimensions.
        expect(bbox!.width!).toBeGreaterThanOrEqual(220 - BOUNDING_BOX_PADDING);
    });
});

import { LINEAGE_ENTITY_NODE_NAME } from '@app/lineageV3/LineageEntityNode/LineageEntityNode';
import { LINEAGE_TRANSFORMATION_NODE_NAME } from '@app/lineageV3/LineageTransformationNode/LineageTransformationNode';
import {
    FetchStatus,
    LINEAGE_FILTER_TYPE,
    LineageEdge,
    LineageEntity,
    LineageFilter,
    LineageNode,
    LineageTableEdgeData,
} from '@app/lineageV3/common';
import NodeBuilder from '@app/lineageV3/useComputeGraph/NodeBuilder';

import { EntityType, LineageDirection } from '@types';

describe('NodeBuilder', () => {
    const createMockNode = (
        id: string,
        type: EntityType | typeof LINEAGE_FILTER_TYPE,
        direction?: LineageDirection,
    ): LineageNode => {
        if (type === LINEAGE_FILTER_TYPE) {
            return {
                id,
                type: LINEAGE_FILTER_TYPE,
                direction: direction!,
                isExpanded: { [LineageDirection.Upstream]: true, [LineageDirection.Downstream]: true },
                fetchStatus: {
                    [LineageDirection.Upstream]: FetchStatus.UNFETCHED,
                    [LineageDirection.Downstream]: FetchStatus.UNFETCHED,
                },
                filters: {
                    [LineageDirection.Upstream]: { facetFilters: new Map() },
                    [LineageDirection.Downstream]: { facetFilters: new Map() },
                },
                parent: 'root',
                contents: [],
                allChildren: new Set(),
                shown: new Set(),
                limit: 4,
            } as LineageFilter;
        }
        return {
            id,
            urn: id,
            type: type as EntityType,
            direction,
            isExpanded: { [LineageDirection.Upstream]: true, [LineageDirection.Downstream]: true },
            fetchStatus: {
                [LineageDirection.Upstream]: FetchStatus.UNFETCHED,
                [LineageDirection.Downstream]: FetchStatus.UNFETCHED,
            },
            filters: {
                [LineageDirection.Upstream]: { facetFilters: new Map() },
                [LineageDirection.Downstream]: { facetFilters: new Map() },
            },
        } as LineageEntity;
    };

    const createMockEntity = (urn: string, type: EntityType): LineageEntity => ({
        id: urn,
        urn,
        type,
        entity: {
            urn,
            type,
            name: urn,
            exists: true,
        },
        isExpanded: { [LineageDirection.Upstream]: true, [LineageDirection.Downstream]: true },
        fetchStatus: {
            [LineageDirection.Upstream]: FetchStatus.UNFETCHED,
            [LineageDirection.Downstream]: FetchStatus.UNFETCHED,
        },
        filters: {
            [LineageDirection.Upstream]: { facetFilters: new Map() },
            [LineageDirection.Downstream]: { facetFilters: new Map() },
        },
    });

    it('should correctly categorize nodes by type', () => {
        const rootUrn = 'root';
        const rootType = EntityType.Dataset;
        const roots = [createMockEntity(rootUrn, rootType)];

        const datasetNode = createMockNode('dataset1', EntityType.Dataset);
        const transformNode = createMockNode('transform1', EntityType.DataJob);
        const filterNode = createMockNode('filter1', LINEAGE_FILTER_TYPE, LineageDirection.Downstream);

        const nodes = [datasetNode, transformNode, filterNode];
        const parents = new Map<string, Set<string>>();

        const builder = new NodeBuilder(rootUrn, rootType, roots, nodes, parents);

        expect(builder.entities).toContain(datasetNode);
        expect(builder.transformations).toContain(transformNode);
        expect(builder.filterNodes).toContain(filterNode);
    });

    it('should create nodes with correct properties', () => {
        const rootUrn = 'root';
        const rootType = EntityType.Dataset;
        const roots = [createMockEntity(rootUrn, rootType)];

        const datasetNode = createMockNode('dataset1', EntityType.Dataset, LineageDirection.Downstream);
        const transformNode = createMockNode('transform1', EntityType.DataJob, LineageDirection.Downstream);

        const nodes = [datasetNode, transformNode];
        const parents = new Map<string, Set<string>>();
        const adjacencyList = {
            [LineageDirection.Upstream]: new Map<string, Set<string>>(),
            [LineageDirection.Downstream]: new Map<string, Set<string>>([
                ['root', new Set(['dataset1'])],
                ['dataset1', new Set(['transform1'])],
            ]),
        };
        const edges = new Map();

        const builder = new NodeBuilder(rootUrn, rootType, roots, nodes, parents);
        const createdNodes = builder.createNodes(
            { adjacencyList, edges },
            false,
            new Map([[LineageDirection.Downstream, [0, 0]]]),
        );

        expect(createdNodes).toHaveLength(2);

        const datasetVisualNode = createdNodes.find((n) => n.id === 'dataset1');
        expect(datasetVisualNode).toBeDefined();
        expect(datasetVisualNode?.type).toBe(LINEAGE_ENTITY_NODE_NAME);
        expect(datasetVisualNode?.selectable).toBe(true);

        const transformVisualNode = createdNodes.find((n) => n.id === 'transform1');
        expect(transformVisualNode).toBeDefined();
        expect(transformVisualNode?.type).toBe(LINEAGE_TRANSFORMATION_NODE_NAME);
        expect(transformVisualNode?.selectable).toBe(true);
    });

    it('should create edges with correct properties', () => {
        const rootUrn = 'root';
        const rootType = EntityType.Dataset;
        const roots = [createMockEntity(rootUrn, rootType)];

        const datasetNode = createMockNode('dataset1', EntityType.Dataset, LineageDirection.Downstream);
        const transformNode = createMockNode('transform1', EntityType.DataJob, LineageDirection.Downstream);

        const nodes = [datasetNode, transformNode];
        const parents = new Map<string, Set<string>>();
        const edges = new Map<string, LineageTableEdgeData>([
            ['root-:-dataset1', { isDisplayed: true, originalId: 'root-:-dataset1' }],
            ['dataset1-:-transform1', { isDisplayed: true, originalId: 'dataset1-:-transform1' }],
        ]);

        const builder = new NodeBuilder(rootUrn, rootType, roots, nodes, parents);
        const createdEdges = builder.createEdges(edges, new Map([[LineageDirection.Downstream, [0, 0]]]));

        expect(createdEdges).toHaveLength(2);
        expect(createdEdges[0].source).toBe('root');
        expect(createdEdges[0].target).toBe('dataset1');
        expect(createdEdges[1].source).toBe('dataset1');
        expect(createdEdges[1].target).toBe('transform1');
    });

    it('should compute correct node positions', () => {
        const rootUrn = 'root';
        const rootType = EntityType.Dataset;
        const roots = [createMockEntity(rootUrn, rootType)];

        const nodes = [
            ...roots,
            createMockNode('dataset1', EntityType.Dataset, LineageDirection.Downstream),
            createMockNode('transform1', EntityType.DataJob, LineageDirection.Downstream),
            createMockNode('dataset2', EntityType.Dataset, LineageDirection.Downstream),
            createMockNode('dataset4', EntityType.Dataset, LineageDirection.Upstream),
            createMockNode('transform2', EntityType.DataJob, LineageDirection.Upstream),
            createMockNode('dataset5', EntityType.Dataset, LineageDirection.Upstream),
        ];
        const parents = new Map<string, Set<string>>([
            ['root', new Set()],
            ['dataset1', new Set(['root'])],
            ['dataset2', new Set(['root'])],
            ['transform1', new Set(['dataset1'])],
            ['dataset4', new Set(['root'])],
            ['transform2', new Set(['dataset4'])],
            ['dataset5', new Set(['transform2'])],
        ]);

        const adjacencyList = {
            [LineageDirection.Upstream]: new Map<string, Set<string>>([
                ['dataset1', new Set(['root'])],
                ['dataset2', new Set(['root'])],
                ['transform1', new Set(['dataset1'])],
                ['root', new Set(['dataset4'])],
                ['dataset4', new Set(['transform2'])],
                ['transform2', new Set(['dataset5'])],
            ]),
            [LineageDirection.Downstream]: new Map<string, Set<string>>([
                ['root', new Set(['dataset1', 'dataset2'])],
                ['dataset1', new Set(['transform1'])],
                ['dataset5', new Set(['transform2'])],
                ['transform2', new Set(['dataset4'])],
                ['dataset4', new Set(['root'])],
            ]),
        };
        const edges = new Map<string, LineageEdge>([
            ['root-:-dataset1', { isDisplayed: true }],
            ['root-:-dataset2', { isDisplayed: true }],
            ['dataset1-:-transform1', { isDisplayed: true }],
            ['dataset5-:-transform2', { isDisplayed: true }],
            ['transform2-:-dataset4', { isDisplayed: true }],
            ['dataset4-:-root', { isDisplayed: true }],
        ]);

        const builder = new NodeBuilder(rootUrn, rootType, roots, nodes, parents);
        const createdNodes = builder.createNodes({ adjacencyList, edges }, false, new Map());

        // Verify that nodes exist and are positioned correctly
        const dataset1Node = createdNodes.find((n) => n.id === 'dataset1');
        const dataset2Node = createdNodes.find((n) => n.id === 'dataset2');
        const transform1Node = createdNodes.find((n) => n.id === 'transform1');
        const dataset4Node = createdNodes.find((n) => n.id === 'dataset4');
        const transform2Node = createdNodes.find((n) => n.id === 'transform2');
        const dataset5Node = createdNodes.find((n) => n.id === 'dataset5');

        expect(dataset1Node?.position.x).toEqual(dataset2Node?.position.x);
        expect(dataset1Node?.position.y).toBeLessThan(dataset2Node?.position.y ?? -Infinity);
        expect(transform1Node?.position.x).toBeGreaterThan(dataset1Node?.position.x ?? Infinity);
        expect(dataset4Node?.position.y).toEqual(dataset5Node?.position.y);
        expect(transform2Node?.position.y).within(
            (dataset4Node?.position.y || 0) - 20,
            (dataset4Node?.position.y || 0) + 20,
        );
        expect(dataset5Node?.position.x).toBeLessThan(transform2Node?.position.x ?? -Infinity);
        expect(dataset4Node?.position.x).toBeGreaterThan(transform2Node?.position.x ?? Infinity);
    });
});

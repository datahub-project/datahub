import { FetchStatus, LineageEdge, LineageEntity } from '@app/lineageV3/common';
import hideNodes from '@app/lineageV3/useComputeGraph/filterNodes';

import { EntityType, LineageDirection } from '@types';

describe('hideNodes', () => {
    const createMockNode = (urn: string, type: EntityType, entity?: any): LineageEntity => ({
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
    });

    const createMockEdge = (isDisplayed = true): LineageEdge => ({
        isDisplayed,
        isManual: false,
    });

    it('should not modify graph when no filters are applied', () => {
        const rootNode = createMockNode('root', EntityType.Dataset);
        const childNode = createMockNode('child', EntityType.Dataset);
        const nodes = new Map([
            ['root', rootNode],
            ['child', childNode],
        ]);
        const edges = new Map([['root-:-child', createMockEdge()]]);
        const adjacencyList = {
            [LineageDirection.Upstream]: new Map([['child', new Set(['root'])]]),
            [LineageDirection.Downstream]: new Map([['root', new Set(['child'])]]),
        };

        const result = hideNodes(
            'root',
            EntityType.Dataset,
            {
                hideTransformations: false,
                hideDataProcessInstances: false,
                hideGhostEntities: false,
                ignoreSchemaFieldStatus: false,
            },
            { nodes, edges, adjacencyList },
        );

        expect(result.nodes.size).toBe(2);
        expect(result.edges.size).toBe(1);
        expect(result.adjacencyList[LineageDirection.Upstream].get('child')).toEqual(new Set(['root']));
        expect(result.adjacencyList[LineageDirection.Downstream].get('root')).toEqual(new Set(['child']));
    });

    it('should hide ghost entities', () => {
        const rootNode = createMockNode('root', EntityType.Dataset);
        const ghostNode = createMockNode('ghost', EntityType.Dataset, { exists: false });
        const nodes = new Map([
            ['root', rootNode],
            ['ghost', ghostNode],
        ]);
        const edges = new Map([['root-:-ghost', createMockEdge()]]);
        const adjacencyList = {
            [LineageDirection.Upstream]: new Map([['ghost', new Set(['root'])]]),
            [LineageDirection.Downstream]: new Map([['root', new Set(['ghost'])]]),
        };

        const result = hideNodes(
            'root',
            EntityType.Dataset,
            {
                hideTransformations: false,
                hideDataProcessInstances: false,
                hideGhostEntities: true,
                ignoreSchemaFieldStatus: false,
            },
            { nodes, edges, adjacencyList },
        );

        expect(result.nodes.size).toBe(1);
        expect(result.nodes.has('root')).toBe(true);
        expect(result.nodes.has('ghost')).toBe(false);
        expect(result.edges.size).toBe(0);
    });

    it('should hide transformations', () => {
        const rootNode = createMockNode('root', EntityType.Dataset);
        const upstreamTransform = createMockNode('upstreamTransform', EntityType.DataJob);
        const downstreamTransform = createMockNode('downstreamTransform', EntityType.DataJob);
        const upstreamChild = createMockNode('upstreamChild', EntityType.Dataset);
        const downstreamChild = createMockNode('downstreamChild', EntityType.Dataset);
        const nodes = new Map([
            ['root', rootNode],
            ['upstreamTransform', upstreamTransform],
            ['downstreamTransform', downstreamTransform],
            ['upstreamChild', upstreamChild],
            ['downstreamChild', downstreamChild],
        ]);
        const edges = new Map([
            ['root-:-downstreamTransform', createMockEdge()],
            ['downstreamTransform-:-downstreamChild', createMockEdge()],
            ['upstreamChild-:-upstreamTransform', createMockEdge()],
            ['upstreamTransform-:-root', createMockEdge()],
        ]);
        const adjacencyList = {
            [LineageDirection.Upstream]: new Map([
                ['downstreamTransform', new Set(['root'])],
                ['downstreamChild', new Set(['downstreamTransform'])],
                ['upstreamTransform', new Set(['upstreamChild'])],
                ['root', new Set(['upstreamTransform'])],
            ]),
            [LineageDirection.Downstream]: new Map([
                ['root', new Set(['downstreamTransform'])],
                ['downstreamTransform', new Set(['downstreamChild'])],
                ['upstreamChild', new Set(['upstreamTransform'])],
                ['upstreamTransform', new Set(['root'])],
            ]),
        };

        const result = hideNodes(
            'root',
            EntityType.Dataset,
            {
                hideTransformations: true,
                hideDataProcessInstances: false,
                hideGhostEntities: false,
                ignoreSchemaFieldStatus: false,
            },
            { nodes, edges, adjacencyList },
        );

        expect(result.nodes.size).toBe(3);
        expect(result.nodes.has('upstreamTransform')).toBe(false);
        expect(result.nodes.has('downstreamTransform')).toBe(false);
        expect(result.edges.size).toBe(2);
        expect(result.adjacencyList[LineageDirection.Upstream].get('root')).toEqual(new Set(['upstreamChild']));
        expect(result.adjacencyList[LineageDirection.Downstream].get('root')).toEqual(new Set(['downstreamChild']));
    });

    it('should hide data process instances', () => {
        const rootNode = createMockNode('root', EntityType.Dataset);
        const dpiNode = createMockNode('dpi', EntityType.DataProcessInstance);
        const childNode = createMockNode('child', EntityType.Dataset);
        const nodes = new Map([
            ['root', rootNode],
            ['dpi', dpiNode],
            ['child', childNode],
        ]);
        const edges = new Map([
            ['root-:-dpi', createMockEdge()],
            ['dpi-:-child', createMockEdge()],
        ]);
        const adjacencyList = {
            [LineageDirection.Upstream]: new Map([
                ['dpi', new Set(['root'])],
                ['child', new Set(['dpi'])],
            ]),
            [LineageDirection.Downstream]: new Map([
                ['root', new Set(['dpi'])],
                ['dpi', new Set(['child'])],
            ]),
        };

        const result = hideNodes(
            'root',
            EntityType.Dataset,
            {
                hideTransformations: false,
                hideDataProcessInstances: true,
                hideGhostEntities: false,
                ignoreSchemaFieldStatus: false,
            },
            { nodes, edges, adjacencyList },
        );

        expect(result.nodes.size).toBe(2);
        expect(result.nodes.has('dpi')).toBe(false);
        expect(result.edges.size).toBe(1);
        expect(result.adjacencyList[LineageDirection.Upstream].get('child')).toEqual(new Set(['root']));
        expect(result.adjacencyList[LineageDirection.Downstream].get('root')).toEqual(new Set(['child']));
    });

    it('should handle custom filter function', () => {
        const rootNode = createMockNode('root', EntityType.Dataset);
        const nodeA = createMockNode('nodeA', EntityType.Dataset);
        const nodeB = createMockNode('nodeB', EntityType.Dataset);
        const nodes = new Map([
            ['root', rootNode],
            ['nodeA', nodeA],
            ['nodeB', nodeB],
        ]);
        const edges = new Map([
            ['root-:-nodeA', createMockEdge()],
            ['root-:-nodeB', createMockEdge()],
        ]);
        const adjacencyList = {
            [LineageDirection.Upstream]: new Map([
                ['nodeA', new Set(['root'])],
                ['nodeB', new Set(['root'])],
            ]),
            [LineageDirection.Downstream]: new Map([['root', new Set(['nodeA', 'nodeB'])]]),
        };

        const result = hideNodes(
            'root',
            EntityType.Dataset,
            {
                hideTransformations: false,
                hideDataProcessInstances: false,
                hideGhostEntities: false,
                ignoreSchemaFieldStatus: false,
            },
            { nodes, edges, adjacencyList },
            (node) => node.urn !== 'nodeB',
        );

        expect(result.nodes.size).toBe(2);
        expect(result.nodes.has('nodeB')).toBe(false);
        expect(result.edges.size).toBe(1);
        expect(result.adjacencyList[LineageDirection.Upstream].get('nodeA')).toEqual(new Set(['root']));
        expect(result.adjacencyList[LineageDirection.Downstream].get('root')).toEqual(new Set(['nodeA']));
    });

    it('should handle hidden edges', () => {
        const rootNode = createMockNode('root', EntityType.Dataset);
        const childNode = createMockNode('child', EntityType.Dataset);
        const nodes = new Map([
            ['root', rootNode],
            ['child', childNode],
        ]);
        const edges = new Map([['root-:-child', createMockEdge(false)]]);
        const adjacencyList = {
            [LineageDirection.Upstream]: new Map([['child', new Set(['root'])]]),
            [LineageDirection.Downstream]: new Map([['root', new Set(['child'])]]),
        };

        const result = hideNodes(
            'root',
            EntityType.Dataset,
            {
                hideTransformations: false,
                hideDataProcessInstances: false,
                hideGhostEntities: false,
                ignoreSchemaFieldStatus: false,
            },
            { nodes, edges, adjacencyList },
        );

        expect(result.nodes.size).toBe(2);
        expect(result.edges.size).toBe(0);
        expect(result.adjacencyList[LineageDirection.Upstream].get('child')).toBeUndefined();
        expect(result.adjacencyList[LineageDirection.Downstream].get('root')).toBeUndefined();
    });

    it('should handle complex transformation hiding with multiple paths and cycles', () => {
        // Create a complex graph with multiple paths and transformation chains:
        // dataset1 -> transform1 -> transform2 -> dataset2 -> transform3 -> transform4 -> dataset3
        // dataset1 -> transform5 -> transform6 -> dataset3, dataset4
        // dataset2 -> transform7 -> transform8 -> dataset1 (cycle)

        const nodes = new Map([
            ['dataset1', createMockNode('dataset1', EntityType.Dataset)],
            ['dataset2', createMockNode('dataset2', EntityType.Dataset)],
            ['dataset3', createMockNode('dataset3', EntityType.Dataset)],
            ['dataset4', createMockNode('dataset4', EntityType.Dataset)],
            ['transform1', createMockNode('transform1', EntityType.DataJob)],
            ['transform2', createMockNode('transform2', EntityType.DataJob)],
            ['transform3', createMockNode('transform3', EntityType.DataJob)],
            ['transform4', createMockNode('transform4', EntityType.DataJob)],
            ['transform5', createMockNode('transform5', EntityType.DataJob)],
            ['transform6', createMockNode('transform6', EntityType.DataJob)],
            ['transform7', createMockNode('transform7', EntityType.DataJob)],
            ['transform8', createMockNode('transform8', EntityType.DataJob)],
        ]);

        const edges = new Map([
            ['dataset1-:-transform1', createMockEdge()],
            ['transform1-:-transform2', createMockEdge()],
            ['transform2-:-dataset2', createMockEdge()],
            ['dataset2-:-transform3', createMockEdge()],
            ['transform3-:-transform4', createMockEdge()],
            ['transform4-:-dataset3', createMockEdge()],
            ['dataset1-:-transform5', createMockEdge()],
            ['transform5-:-transform6', createMockEdge()],
            ['transform5-:-dataset3', createMockEdge()],
            ['transform6-:-dataset4', createMockEdge()],
            ['dataset2-:-transform7', createMockEdge()],
            ['transform7-:-transform8', createMockEdge()],
            ['transform8-:-dataset1', createMockEdge()],
        ]);

        const adjacencyList = {
            [LineageDirection.Upstream]: new Map([
                ['transform1', new Set(['dataset1'])],
                ['transform2', new Set(['transform1'])],
                ['transform3', new Set(['dataset2'])],
                ['transform4', new Set(['transform3'])],
                ['transform5', new Set(['dataset1'])],
                ['transform6', new Set(['transform5'])],
                ['transform7', new Set(['dataset2'])],
                ['transform8', new Set(['transform7'])],
                ['dataset1', new Set(['transform8'])],
                ['dataset2', new Set(['transform2'])],
                ['dataset3', new Set(['transform4', 'transform5'])],
                ['dataset4', new Set(['transform6'])],
            ]),
            [LineageDirection.Downstream]: new Map([
                ['dataset1', new Set(['transform1', 'transform5'])],
                ['dataset2', new Set(['transform3', 'transform7'])],
                ['transform1', new Set(['transform2'])],
                ['transform2', new Set(['dataset2'])],
                ['transform3', new Set(['transform4'])],
                ['transform4', new Set(['dataset3'])],
                ['transform5', new Set(['transform6', 'dataset3'])],
                ['transform6', new Set(['dataset4'])],
                ['transform7', new Set(['transform8'])],
                ['transform8', new Set(['dataset1'])],
            ]),
        };

        const result = hideNodes(
            'dataset1',
            EntityType.Dataset,
            {
                hideTransformations: true,
                hideDataProcessInstances: false,
                hideGhostEntities: false,
                ignoreSchemaFieldStatus: false,
            },
            { nodes, edges, adjacencyList },
        );

        // After hiding transformations, we should have:
        // 1. Only dataset nodes remaining
        expect(result.nodes.size).toBe(4);
        expect(result.nodes.has('transform1')).toBe(false);
        expect(result.nodes.has('transform2')).toBe(false);
        expect(result.nodes.has('transform3')).toBe(false);
        expect(result.nodes.has('transform4')).toBe(false);
        expect(result.nodes.has('transform5')).toBe(false);
        expect(result.nodes.has('transform6')).toBe(false);
        expect(result.nodes.has('transform7')).toBe(false);
        expect(result.nodes.has('transform8')).toBe(false);

        // 2. Edges should connect through the removed transformation nodes
        // We expect direct connections between datasets:
        // dataset1 <-> dataset2
        // dataset1 -> dataset3
        // dataset2 -> dataset3
        // dataset1 -> dataset4
        expect(result.edges.size).toBe(5);

        // 3. Adjacency list should reflect the new connections
        expect(result.adjacencyList[LineageDirection.Upstream].get('dataset1')).toEqual(new Set(['dataset2']));
        expect(result.adjacencyList[LineageDirection.Upstream].get('dataset2')).toEqual(new Set(['dataset1']));
        expect(result.adjacencyList[LineageDirection.Upstream].get('dataset3')).toEqual(
            new Set(['dataset1', 'dataset2']),
        );
        expect(result.adjacencyList[LineageDirection.Upstream].get('dataset4')).toEqual(new Set(['dataset1']));

        expect(result.adjacencyList[LineageDirection.Downstream].get('dataset1')).toEqual(
            new Set(['dataset2', 'dataset3', 'dataset4']),
        );
        expect(result.adjacencyList[LineageDirection.Downstream].get('dataset2')).toEqual(
            new Set(['dataset1', 'dataset3']),
        );
        expect(result.adjacencyList[LineageDirection.Downstream].get('dataset3')?.size || 0).toEqual(0);
        expect(result.adjacencyList[LineageDirection.Downstream].get('dataset4')?.size || 0).toEqual(0);
    });
});

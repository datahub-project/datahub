import { renderHook } from '@testing-library/react-hooks';
import { describe, expect, it } from 'vitest';

import { LINEAGE_ANNOTATION_NODE } from '@app/lineageV3/LineageAnnotationNode/LineageAnnotationNode';
import useAddAnnotationNodes from '@app/lineageV3/LineageAnnotationNode/useAddAnnotationNodes';
import { LINEAGE_ENTITY_NODE_NAME } from '@app/lineageV3/LineageEntityNode/LineageEntityNode';

import { EntityType } from '@types';

describe('useAddAnnotationNodes', () => {
    it('should add level info to nodes and add annotations where hiddenEntities > 0', () => {
        const { result } = renderHook(() => useAddAnnotationNodes());

        const filteredNodes = [
            {
                id: 'urn:li:dataset1',
                type: LINEAGE_ENTITY_NODE_NAME,
                position: { x: 10, y: 20 },
                data: { urn: 'urn:li:dataset1', type: EntityType.Dataset, name: 'Dataset 1' },
            },
            {
                id: 'urn:li:dataset2',
                type: LINEAGE_ENTITY_NODE_NAME,
                position: { x: 30, y: 40 },
                data: { urn: 'urn:li:dataset2', type: EntityType.Dataset, name: 'Dataset 2' },
            },
        ];

        const levelsMap = new Map([
            ['urn:li:dataset1', 2],
            ['urn:li:dataset2', 3],
        ]);
        const levelsInfo = {
            2: { shownEntities: 1, totalEntities: 2, hiddenEntities: 1 },
            3: { shownEntities: 1, totalEntities: 1, hiddenEntities: 0 },
        };

        const resultNodes = result.current(filteredNodes, levelsInfo, levelsMap);

        expect(resultNodes.find((n) => n.id === 'urn:li:dataset1')?.data.level).toBe(2);
        expect(resultNodes.find((n) => n.id === 'urn:li:dataset2')?.data.level).toBe(3);

        const annotationNode = resultNodes.find((n) => n.id === 'annotation-2');
        expect(annotationNode).toBeDefined();
        expect(annotationNode?.type).toBe(LINEAGE_ANNOTATION_NODE);
        expect(annotationNode?.position.x).toBe(10);
        expect(annotationNode?.position.y).toBe(20 - 40);
        expect(annotationNode?.data.label).toBe('1 of 2 shown');
    });

    it('should not add annotations if hiddenEntities is zero or less', () => {
        const { result } = renderHook(() => useAddAnnotationNodes());

        const nodes = [
            {
                id: 'urn:li:dataset3',
                type: LINEAGE_ENTITY_NODE_NAME,
                position: { x: 50, y: 60 },
                data: { urn: 'urn:li:dataset3', type: EntityType.Dataset, name: 'Dataset 3' },
            },
        ];

        const levelsMap = new Map([['urn:li:dataset3', 1]]);
        const levelsInfo = {
            1: { shownEntities: 1, totalEntities: 1, hiddenEntities: 0 },
        };

        const resultNodes = result.current(nodes, levelsInfo, levelsMap);

        // No annotation nodes should be added
        expect(resultNodes.length).toBe(nodes.length);
    });

    it('should correctly position annotation for multiple nodes at same level', () => {
        const { result } = renderHook(() => useAddAnnotationNodes());

        const nodes = [
            {
                id: 'urn:li:datasetA',
                type: LINEAGE_ENTITY_NODE_NAME,
                position: { x: 70, y: 80 },
                data: { urn: 'urn:li:datasetA', type: EntityType.Dataset, name: 'Dataset A' },
            },
            {
                id: 'urn:li:datasetB',
                type: LINEAGE_ENTITY_NODE_NAME,
                position: { x: 100, y: 120 },
                data: { urn: 'urn:li:datasetB', type: EntityType.Dataset, name: 'Dataset B' },
            },
        ];

        const levelsMap = new Map([
            ['urn:li:datasetA', 4],
            ['urn:li:datasetB', 4],
        ]);
        const levelsInfo = {
            4: { shownEntities: 2, totalEntities: 4, hiddenEntities: 2 },
        };

        const resultNodes = result.current(nodes, levelsInfo, levelsMap);

        const annotation = resultNodes.find((n) => n.id === 'annotation-4');

        expect(annotation).toBeDefined();
        expect(annotation?.position.x).toBe(70);
        expect(annotation?.position.y).toBe(80 - 40);
    });

    it('should handle case with no entity nodes at a level', () => {
        const { result } = renderHook(() => useAddAnnotationNodes());

        const nodes = [
            {
                id: 'urn:li:datasetX',
                type: 'other_type',
                position: { x: 10, y: 10 },
                data: { urn: 'urn:li:datasetX', type: EntityType.Dataset, name: 'Dataset X' },
            },
        ];

        const levelsMap = new Map([['urn:li:datasetX', 1]]);
        const levelsInfo = {
            1: { shownEntities: 1, totalEntities: 6, hiddenEntities: 5 },
        };

        const resultNodes = result.current(nodes, levelsInfo, levelsMap);

        expect(resultNodes.length).toBe(nodes.length);
    });

    it('should add level property even if levelsMap has no entry for a node', () => {
        const { result } = renderHook(() => useAddAnnotationNodes());

        const filteredNodes = [
            {
                id: 'urn:li:datasetNoLevel',
                type: LINEAGE_ENTITY_NODE_NAME,
                position: { x: 0, y: 0 },
                data: { urn: 'urn:li:datasetNoLevel', type: EntityType.Dataset, name: 'No Level' },
            },
        ];

        const levelsMap = new Map<string, number>(); // empty, no levels

        const levelsInfo = {};

        const resultNodes = result.current(filteredNodes, levelsInfo, levelsMap);

        // Default level 0 added
        expect(resultNodes.find((n) => n.id === 'urn:li:datasetNoLevel')?.data.level).toBe(0);
    });
});

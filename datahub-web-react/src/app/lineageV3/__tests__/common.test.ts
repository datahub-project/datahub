import {
    NodeContext,
    addToAdjacencyList,
    cloneAdjacencyList,
    generateIgnoreAsHops,
    isTransformational,
} from '@app/lineageV3/common';

import { EntityType, LineageDirection } from '@types';

const A = 'urn:li:dataset:A';
const B = 'urn:li:dataset:B';
const C = 'urn:li:dataset:C';

function adjacencyList(): NodeContext['adjacencyList'] {
    const list: NodeContext['adjacencyList'] = {
        [LineageDirection.Upstream]: new Map(),
        [LineageDirection.Downstream]: new Map(),
    };
    addToAdjacencyList(list, LineageDirection.Downstream, A, B);
    return list;
}

describe('cloneAdjacencyList', () => {
    it('copies both directions', () => {
        const original = adjacencyList();

        expect(cloneAdjacencyList(original)).toEqual(original);
    });

    it('does not share neighbor sets with the original', () => {
        const original = adjacencyList();
        const clone = cloneAdjacencyList(original);

        addToAdjacencyList(clone, LineageDirection.Downstream, A, C);
        expect(clone[LineageDirection.Downstream].get(A)).toEqual(new Set([B, C]));
        expect(original[LineageDirection.Downstream].get(A)).toEqual(new Set([B]));
        expect(original[LineageDirection.Upstream].has(C)).toBe(false);
    });
});

describe('transformation platforms', () => {
    const dataset = (platform: string) => `urn:li:dataset:(urn:li:dataPlatform:${platform},db.orders,PROD)`;

    it.each(['dbt', 'sqlmesh'])('draws %s datasets and their columns as transformations', (platform) => {
        expect(isTransformational({ urn: dataset(platform), type: EntityType.Dataset }, EntityType.Dataset)).toBe(true);
        expect(
            isTransformational(
                { urn: `urn:li:schemaField:(${dataset(platform)},order_id)`, type: EntityType.SchemaField },
                EntityType.Dataset,
            ),
        ).toBe(true);
    });

    it('draws warehouse datasets as datasets', () => {
        expect(isTransformational({ urn: dataset('snowflake'), type: EntityType.Dataset }, EntityType.Dataset)).toBe(
            false,
        );
    });

    it('walks lineage search through dbt and SQLMesh datasets and columns', () => {
        const hops = generateIgnoreAsHops(EntityType.Dataset);
        [EntityType.Dataset, EntityType.SchemaField].forEach((entityType) => {
            expect(hops.find((hop) => hop.entityType === entityType)?.platforms).toEqual([
                'urn:li:dataPlatform:dbt',
                'urn:li:dataPlatform:sqlmesh',
            ]);
        });
    });
});

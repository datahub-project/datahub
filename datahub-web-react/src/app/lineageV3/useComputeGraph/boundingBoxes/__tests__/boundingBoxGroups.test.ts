import { NodeContext, createEdgeId } from '@app/lineageV3/common';
import { assignQueriesToGroups } from '@app/lineageV3/useComputeGraph/boundingBoxes/boundingBoxGroups';
import { BoundingBoxGroup, GraphStore } from '@app/lineageV3/useComputeGraph/boundingBoxes/boundingBoxes.types';

import { EntityType, LineageDirection } from '@types';

const HOME = 'urn:li:dataProduct:home';
const OTHER = 'urn:li:dataProduct:other';
const Q = 'urn:li:query:Q';
const A = 'urn:li:dataset:A';
const B = 'urn:li:dataset:B';
const C = 'urn:li:dataset:C';
const D = 'urn:li:dataset:D';

function group(urn: string, memberUrns: string[]): BoundingBoxGroup {
    return { urn, type: EntityType.DataProduct, memberUrns: new Set(memberUrns), queryUrns: new Set() };
}

function store(edges: [string, string, string | undefined][]): GraphStore {
    const edgeMap: NodeContext['edges'] = new Map(
        edges.map(([upstream, downstream, via]) => [createEdgeId(upstream, downstream), { isDisplayed: true, via }]),
    );
    return {
        nodes: new Map(),
        edges: edgeMap,
        adjacencyList: { [LineageDirection.Upstream]: new Map(), [LineageDirection.Downstream]: new Map() },
        rootType: EntityType.DataProduct,
    };
}

describe('assignQueriesToGroups', () => {
    it('places a query connecting two members of the same data product inside its box', () => {
        const groups = new Map([[HOME, group(HOME, [A, B])]]);
        assignQueriesToGroups(groups, store([[A, B, Q]]), new Set([A, B, Q]));

        expect(groups.get(HOME)?.queryUrns).toEqual(new Set([Q]));
    });

    it('leaves a query that only borders data products outside them all', () => {
        // A (home) -> Q -> C (other): the query is downstream of one product and upstream of another
        const groups = new Map([
            [HOME, group(HOME, [A, B])],
            [OTHER, group(OTHER, [C, D])],
        ]);
        assignQueriesToGroups(groups, store([[A, C, Q]]), new Set([A, B, C, D, Q]));

        expect(groups.get(HOME)?.queryUrns.size).toBe(0);
        expect(groups.get(OTHER)?.queryUrns.size).toBe(0);
    });

    it('picks the data product whose members the query connects most of', () => {
        // Q connects two members of HOME and three of OTHER
        const groups = new Map([
            [HOME, group(HOME, [A, B])],
            [OTHER, group(OTHER, [B, C, D])],
        ]);
        const edges = store([
            [A, B, Q],
            [B, C, Q],
            [C, D, Q],
        ]);
        assignQueriesToGroups(groups, edges, new Set([A, B, C, D, Q]));

        expect(groups.get(HOME)?.queryUrns.size).toBe(0);
        expect(groups.get(OTHER)?.queryUrns).toEqual(new Set([Q]));
    });

    it('ignores lineage through members that are not displayed', () => {
        const groups = new Map([[HOME, group(HOME, [A, B])]]);
        assignQueriesToGroups(groups, store([[A, B, Q]]), new Set([A, Q]));

        expect(groups.get(HOME)?.queryUrns.size).toBe(0);
    });
});

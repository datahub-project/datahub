import { NodeContext, addToAdjacencyList, cloneAdjacencyList } from '@app/lineageV3/common';

import { LineageDirection } from '@types';

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

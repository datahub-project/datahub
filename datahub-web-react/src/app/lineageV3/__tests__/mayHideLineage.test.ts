import { FetchStatus, mayHideLineage } from '@app/lineageV3/common';

import { LineageDirection } from '@types';

const UP = LineageDirection.Upstream;
const DOWN = LineageDirection.Downstream;

function node(status: FetchStatus, isExpandedUpstream: boolean) {
    return {
        fetchStatus: { [UP]: status, [DOWN]: FetchStatus.UNNEEDED },
        isExpanded: { [UP]: isExpandedUpstream, [DOWN]: false },
    };
}

describe('mayHideLineage', () => {
    it('is true while lineage is unfetched or loading, as an expand control is shown', () => {
        expect(mayHideLineage(UP, node(FetchStatus.UNFETCHED, false), true, false)).toBe(true);
        expect(mayHideLineage(UP, node(FetchStatus.LOADING, false), true, false)).toBe(true);
    });

    it('is true while lineage is contracted, as an expand control is shown', () => {
        expect(mayHideLineage(UP, node(FetchStatus.COMPLETE, false), true, false)).toBe(true);
    });

    it('is true when expanded children are filtered out, as the contract control counts them', () => {
        expect(mayHideLineage(UP, node(FetchStatus.COMPLETE, true), true, true)).toBe(true);
    });

    it('is false once every child is expanded onto the graph', () => {
        expect(mayHideLineage(UP, node(FetchStatus.COMPLETE, true), true, false)).toBe(false);
    });

    it('is false without children in that direction, where no control is shown', () => {
        expect(mayHideLineage(UP, node(FetchStatus.UNFETCHED, false), false, true)).toBe(false);
        expect(mayHideLineage(DOWN, node(FetchStatus.UNFETCHED, false), false, false)).toBe(false);
    });

    it('is false for a direction that is not being explored', () => {
        // Nodes upstream of the home node are not searched downstream, so no control is shown
        expect(mayHideLineage(DOWN, node(FetchStatus.COMPLETE, true), true, false)).toBe(false);
    });
});

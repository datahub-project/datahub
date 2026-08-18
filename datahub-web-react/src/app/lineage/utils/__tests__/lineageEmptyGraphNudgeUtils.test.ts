import { describe, expect, it } from 'vitest';

import {
    getLineageCountTotal,
    getLineageEmptyGraphNudgeState,
    isLineageGraphEmpty,
    isLineageGraphLoaded,
} from '@app/lineage/utils/lineageEmptyGraphNudgeUtils';

import { LineageDirection } from '@types';

const FETCH_STATUS_COMPLETE = 'COMPLETE';

describe('lineageEmptyGraphNudgeUtils', () => {
    it('detects when lineage graph loading is complete', () => {
        expect(
            isLineageGraphLoaded({
                [LineageDirection.Upstream]: FETCH_STATUS_COMPLETE,
                [LineageDirection.Downstream]: FETCH_STATUS_COMPLETE,
            }),
        ).toBe(true);
        expect(
            isLineageGraphLoaded({
                [LineageDirection.Upstream]: 'LOADING',
                [LineageDirection.Downstream]: FETCH_STATUS_COMPLETE,
            }),
        ).toBe(false);
    });

    it('detects when lineage graph has no neighbors', () => {
        const adjacencyList = {
            [LineageDirection.Upstream]: new Map<string, Set<string>>(),
            [LineageDirection.Downstream]: new Map<string, Set<string>>(),
        };

        expect(isLineageGraphEmpty('urn:li:dataset:(test)', adjacencyList)).toBe(true);

        adjacencyList[LineageDirection.Downstream].set(
            'urn:li:dataset:(test)',
            new Set(['urn:li:dataset:(downstream)']),
        );
        expect(isLineageGraphEmpty('urn:li:dataset:(test)', adjacencyList)).toBe(false);
    });

    it('sums lineage counts across directions', () => {
        expect(getLineageCountTotal(2, 3)).toBe(5);
        expect(getLineageCountTotal(undefined, 1)).toBe(1);
    });

    it('shows a time-filter nudge when filtered lineage is smaller than all-time lineage', () => {
        expect(
            getLineageEmptyGraphNudgeState({
                isLoaded: true,
                isEmpty: true,
                isTimeFilterActive: true,
                showGhostEntities: false,
                allTimeTotal: 4,
                filteredTotal: 0,
            }),
        ).toEqual({
            show: true,
            reasons: ['timeFilter'],
        });
    });

    it('shows a hidden-edges nudge when lineage exists but ghost entities are hidden', () => {
        expect(
            getLineageEmptyGraphNudgeState({
                isLoaded: true,
                isEmpty: true,
                isTimeFilterActive: false,
                showGhostEntities: false,
                allTimeTotal: 2,
                filteredTotal: 2,
            }),
        ).toEqual({
            show: true,
            reasons: ['hiddenEdges'],
        });
    });

    it('does not show a nudge when lineage truly does not exist', () => {
        expect(
            getLineageEmptyGraphNudgeState({
                isLoaded: true,
                isEmpty: true,
                isTimeFilterActive: true,
                showGhostEntities: false,
                allTimeTotal: 0,
                filteredTotal: 0,
            }),
        ).toEqual({
            show: false,
            reasons: [],
        });
    });
});

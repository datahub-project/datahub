import { describe, expect, it } from 'vitest';

import { getWrappedIndex, isLineageFilterActive } from '@app/lineage/controls/lineageControlsUtils';

describe('isLineageFilterActive', () => {
    it('is inactive when nothing deviates from the defaults', () => {
        expect(
            isLineageFilterActive({
                hideTransformations: false,
                showDataProcessInstances: true,
                showGhostEntities: false,
            }),
        ).toBe(false);
    });

    it('is active when transformations are hidden', () => {
        expect(
            isLineageFilterActive({
                hideTransformations: true,
                showDataProcessInstances: true,
                showGhostEntities: false,
            }),
        ).toBe(true);
    });

    it('is active when process instances are hidden', () => {
        expect(
            isLineageFilterActive({
                hideTransformations: false,
                showDataProcessInstances: false,
                showGhostEntities: false,
            }),
        ).toBe(true);
    });

    it('is active when ghost entities are shown', () => {
        expect(
            isLineageFilterActive({
                hideTransformations: false,
                showDataProcessInstances: true,
                showGhostEntities: true,
            }),
        ).toBe(true);
    });
});

describe('getWrappedIndex', () => {
    it('steps forward within bounds', () => {
        expect(getWrappedIndex(0, 1, 3)).toBe(1);
    });

    it('wraps forward past the end to the start', () => {
        expect(getWrappedIndex(2, 1, 3)).toBe(0);
    });

    it('wraps backward past the start to the end', () => {
        expect(getWrappedIndex(0, -1, 3)).toBe(2);
    });

    it('returns NaN for an empty list', () => {
        expect(getWrappedIndex(0, 1, 0)).toBeNaN();
    });
});

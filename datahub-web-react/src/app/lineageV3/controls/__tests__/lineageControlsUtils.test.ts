import { describe, expect, it } from 'vitest';

import { isLineageFilterActive } from '@app/lineageV3/controls/lineageControlsUtils';

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

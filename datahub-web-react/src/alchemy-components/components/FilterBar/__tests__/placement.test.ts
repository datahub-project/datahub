import { describe, expect, it } from 'vitest';

import {
    alignWithinViewport,
    centreWithinViewport,
    placeBelowOrAbove,
    placeBeside,
} from '@components/components/FilterBar/placement';

const VIEWPORT = { top: 0, bottom: 900, left: 0, right: 1440 };
const trigger = (top: number, left = 100) => ({ top, bottom: top + 24, left, right: left + 80 });

describe('placeBelowOrAbove', () => {
    it('opens below the trigger when there is room, and above it near the bottom', () => {
        expect(placeBelowOrAbove(trigger(100), VIEWPORT, 280)).toEqual({ top: 128, left: 100, maxHeight: 760 });
        expect(placeBelowOrAbove(trigger(700), VIEWPORT, 280)).toEqual({ bottom: 204, left: 100, maxHeight: 684 });
    });

    it('shifts left to stay on screen when the trigger is near the right edge', () => {
        expect(placeBelowOrAbove(trigger(100, 1400), VIEWPORT, 280).left).toBe(1440 - 12 - 280);
    });
});

describe('alignWithinViewport', () => {
    it('keeps the top level with the row that opened it', () => {
        expect(alignWithinViewport(400, 100, VIEWPORT)).toBe(400);
    });

    it('shifts up only as far as needed when it would run off the bottom', () => {
        expect(alignWithinViewport(820, 100, VIEWPORT)).toBe(900 - 12 - 100);
    });

    it('stays below the top margin, even when taller than the viewport', () => {
        expect(alignWithinViewport(4, 100, VIEWPORT)).toBe(12);
        expect(alignWithinViewport(400, 2000, VIEWPORT)).toBe(12);
    });
});

describe('centreWithinViewport', () => {
    it('centres on the given point and shifts only as far as the viewport edges need', () => {
        expect(centreWithinViewport(450, 200, VIEWPORT)).toBe(350);
        expect(centreWithinViewport(40, 200, VIEWPORT)).toBe(12);
        expect(centreWithinViewport(880, 200, VIEWPORT)).toBe(688);
        expect(centreWithinViewport(450, 2000, VIEWPORT)).toBe(12);
    });
});

describe('placeBeside', () => {
    const panel = { width: 280, height: 400 };

    it('opens to the right of the trigger, centred on it, as tall as the viewport allows', () => {
        expect(placeBeside(trigger(500), VIEWPORT, panel)).toEqual({ top: 312, left: 184, maxHeight: 876 });
    });

    it('shifts up or down to stay on screen when the trigger is near an edge', () => {
        expect(placeBeside(trigger(860), VIEWPORT, panel).top).toBe(488);
        expect(placeBeside(trigger(10), VIEWPORT, panel).top).toBe(12);
    });

    it('caps a tall menu at the viewport height', () => {
        expect(placeBeside(trigger(500), VIEWPORT, { width: 280, height: 5000 })).toMatchObject({
            top: 12,
            maxHeight: 876,
        });
    });

    it('opens beside the anchor when given, still centred on the trigger', () => {
        const panelBox = { top: 300, bottom: 800, left: 50, right: 420 };
        expect(placeBeside(trigger(500), VIEWPORT, panel, panelBox)).toEqual({ top: 312, left: 428, maxHeight: 876 });
        expect(placeBeside(trigger(500, 1000), VIEWPORT, panel, { ...panelBox, left: 900, right: 1300 }).left).toBe(
            900 - 8 - 280,
        );
    });

    it('opens to the left when only the left has room', () => {
        expect(placeBeside(trigger(500, 1300), VIEWPORT, panel).left).toBe(1300 - 4 - 280);
    });

    it('stays on screen when neither side has room for the menu', () => {
        const narrow = { ...VIEWPORT, right: 500 };
        expect(placeBeside(trigger(500, 250), narrow, panel).left).toBe(12);
        expect(placeBeside(trigger(500, 150), narrow, panel).left).toBe(500 - 12 - 280);
    });
});

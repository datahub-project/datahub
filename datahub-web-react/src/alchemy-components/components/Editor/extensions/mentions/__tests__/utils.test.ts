import { describe, expect, it } from 'vitest';

import {
    DROPDOWN_HEIGHT_THRESHOLD,
    calculateMentionsPlacement,
} from '@components/components/Editor/extensions/mentions/utils';

describe('calculateMentionsPlacement', () => {
    const viewportHeight = 800;

    it('returns bottom-start when there is enough space below cursor', () => {
        // Cursor at top of viewport - plenty of space below
        const cursorBottom = 100;
        expect(calculateMentionsPlacement(cursorBottom, viewportHeight)).toBe('bottom-start');
    });

    it('returns top-start when there is not enough space below cursor', () => {
        // Cursor near bottom of viewport - not enough space below
        const cursorBottom = 600; // 800 - 600 = 200px below, less than 350
        expect(calculateMentionsPlacement(cursorBottom, viewportHeight)).toBe('top-start');
    });

    it('returns bottom-start when space below equals threshold exactly', () => {
        // Edge case: exactly at threshold boundary
        const cursorBottom = viewportHeight - DROPDOWN_HEIGHT_THRESHOLD; // 800 - 350 = 450
        expect(calculateMentionsPlacement(cursorBottom, viewportHeight)).toBe('bottom-start');
    });

    it('returns top-start when space below is one pixel less than threshold', () => {
        // Just under threshold
        const cursorBottom = viewportHeight - DROPDOWN_HEIGHT_THRESHOLD + 1; // 451
        expect(calculateMentionsPlacement(cursorBottom, viewportHeight)).toBe('top-start');
    });

    it('uses custom threshold when provided', () => {
        const cursorBottom = 500;
        const customThreshold = 400;

        // With default threshold (350): 800 - 500 = 300 < 350 → top-start
        expect(calculateMentionsPlacement(cursorBottom, viewportHeight)).toBe('top-start');

        // With custom threshold (400): 800 - 500 = 300 < 400 → top-start
        expect(calculateMentionsPlacement(cursorBottom, viewportHeight, customThreshold)).toBe('top-start');

        // With smaller custom threshold (200): 800 - 500 = 300 >= 200 → bottom-start
        expect(calculateMentionsPlacement(cursorBottom, viewportHeight, 200)).toBe('bottom-start');
    });
});

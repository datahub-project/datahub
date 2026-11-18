import { describe, expect, it } from 'vitest';

import { getElementHeight, getParagraphRowWidths, getTitleWidth } from '@components/components/Skeleton/utils';

describe('getParagraphRowWidths', () => {
    it('defaults to 3 rows under a title and 2 without, shortening the last row', () => {
        expect(getParagraphRowWidths(true, true)).toEqual([undefined, undefined, '61%']);
        expect(getParagraphRowWidths(true, false)).toEqual([undefined, '61%']);
    });

    it('applies a single width to the last row and converts numbers to px', () => {
        expect(getParagraphRowWidths({ rows: 2, width: 120 }, true)).toEqual([undefined, '120px']);
    });

    it('applies an array of widths row by row', () => {
        expect(getParagraphRowWidths({ rows: 3, width: ['50%', 80] }, true)).toEqual(['50%', '80px', undefined]);
    });
});

describe('getTitleWidth', () => {
    it('is shortened only when a paragraph follows, unless a width is given', () => {
        expect(getTitleWidth(true, true)).toBe('38%');
        expect(getTitleWidth(true, false)).toBeUndefined();
        expect(getTitleWidth({ width: 200 }, true)).toBe('200px');
    });
});

describe('getElementHeight', () => {
    it('maps preset sizes and passes numbers through', () => {
        expect(getElementHeight('sm')).toBe(24);
        expect(getElementHeight()).toBe(32);
        expect(getElementHeight(18)).toBe(18);
    });
});

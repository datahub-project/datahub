import { describe, expect, it } from 'vitest';

import {
    getStackedIconOverlap,
    getStackedIconSize,
} from '@app/searchV2/autoCompleteV2/components/icon/DefaultEntityIcon';

describe('getStackedIconSize', () => {
    it('should return the container size when only one logo is shown', () => {
        expect(getStackedIconSize(14, 1)).toBe(14);
    });

    it('should shrink each logo so two overlapped logos fit the container', () => {
        const container = 14;
        const each = getStackedIconSize(container, 2);
        const overlap = getStackedIconOverlap(each);
        expect(each).toBeLessThan(container);
        expect(each + each - overlap).toBeLessThanOrEqual(container + 1);
    });

    it('should keep a readable minimum size', () => {
        expect(getStackedIconSize(10, 4)).toBeGreaterThanOrEqual(8);
    });
});

describe('getStackedIconOverlap', () => {
    it('should scale overlap with icon size', () => {
        expect(getStackedIconOverlap(10)).toBe(3);
        expect(getStackedIconOverlap(8)).toBe(2);
    });
});

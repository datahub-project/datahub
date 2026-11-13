import { describe, expect, it } from 'vitest';

import { DatasetEntity } from '@app/entityV2/dataset/DatasetEntity';

describe('DatasetEntity sidebar sections', () => {
    it('gives every section a unique, stable key that micro frontends can position against', () => {
        const keys = new DatasetEntity().getSidebarSections().map((section) => section.key);
        expect(keys.every(Boolean)).toBe(true);
        expect(new Set(keys).size).toBe(keys.length);
        // The built-in Logical Parent / Physical Children section follows Notes; `after: notes` relies on it.
        expect(keys.indexOf('logical')).toBe(keys.indexOf('notes') + 1);
    });
});

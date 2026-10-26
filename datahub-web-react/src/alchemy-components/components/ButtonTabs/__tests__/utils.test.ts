import { describe, expect, it } from 'vitest';

import {
    appendRenderedTabKey,
    getInitialActiveTabKey,
    shouldRenderTabPanel,
    tabButtonsDefaults,
} from '@components/components/ButtonTabs/utils';

describe('ButtonTabs utils', () => {
    const tabs = [
        { key: 'a', label: 'A', content: null },
        { key: 'b', label: 'B', content: null },
    ];

    it('defaults fit to fill', () => {
        expect(tabButtonsDefaults.fit).toBe('fill');
    });

    it('resolves initial active key from defaultKey or first tab', () => {
        expect(getInitialActiveTabKey(tabs, 'b')).toBe('b');
        expect(getInitialActiveTabKey(tabs)).toBe('a');
        expect(getInitialActiveTabKey([])).toBeUndefined();
    });

    it('appends rendered keys without duplicates', () => {
        expect(appendRenderedTabKey(['a'], 'b')).toEqual(['a', 'b']);
        expect(appendRenderedTabKey(['a', 'b'], 'b')).toEqual(['a', 'b']);
    });

    it('keeps active and previously rendered panels', () => {
        expect(shouldRenderTabPanel('a', 'a', [])).toBe(true);
        expect(shouldRenderTabPanel('b', 'a', ['b'])).toBe(true);
        expect(shouldRenderTabPanel('b', 'a', [])).toBe(false);
    });
});

import { renderHook } from '@testing-library/react-hooks';
import React from 'react';
import { MemoryRouter } from 'react-router-dom';
import { describe, expect, it } from 'vitest';

import { getTabRouteKey, useRoutedTab } from '@app/entityV2/shared/containers/profile/utils';
import { EntityTab } from '@app/entityV2/shared/types';

const tab = (name: string, routeKey?: string): EntityTab => ({ name, routeKey, component: () => null }) as EntityTab;

/**
 * A tab's caption is translated, so it cannot address the tab in a URL. `routeKey` carries the stable
 * segment; tabs that do not set one keep being addressed by caption, exactly as before.
 */
describe('tab route keys', () => {
    it('falls back to the caption when no route key is set', () => {
        expect(getTabRouteKey(tab('Columns'))).toBe('Columns');
        expect(getTabRouteKey(tab('One Click Access', 'mfe-access'))).toBe('mfe-access');
    });

    const routeTo = (path: string, tabs: EntityTab[]) =>
        renderHook(() => useRoutedTab(tabs), {
            wrapper: ({ children }: { children: React.ReactNode }) => (
                <MemoryRouter initialEntries={[path]}>{children}</MemoryRouter>
            ),
        }).result.current;

    it('resolves a tab by its route key, not its caption', () => {
        const tabs = [tab('Columns'), tab('One Click Access', 'mfe-access')];
        expect(routeTo('/dataset/urn:li:dataset:1/mfe-access', tabs)?.name).toBe('One Click Access');
        // the caption no longer addresses a tab that declares a key
        expect(routeTo('/dataset/urn:li:dataset:1/One Click Access', tabs)).toBeUndefined();
    });

    it('keeps addressing built-in tabs by caption', () => {
        const tabs = [tab('Columns'), tab('Lineage')];
        expect(routeTo('/dataset/urn:li:dataset:1/Lineage', tabs)?.name).toBe('Lineage');
    });

    it('survives the caption being translated', () => {
        // same tab, same key, caption rendered in another locale
        const english = [tab('Columns', 'columns')];
        const french = [tab('Colonnes', 'columns')];
        const path = '/dataset/urn:li:dataset:1/columns';
        expect(routeTo(path, english)?.name).toBe('Columns');
        expect(routeTo(path, french)?.name).toBe('Colonnes');
    });
});

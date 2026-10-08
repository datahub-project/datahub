import { describe, expect, it } from 'vitest';

import { lazyProfileComponent } from '@app/entityV2/shared/lazyEntityProfile';

describe('lazyProfileComponent', () => {
    it('gives each wrapper a distinct function identity for sidebar keys', () => {
        const load = () => Promise.resolve({ default: () => null });
        const about = lazyProfileComponent('SidebarAboutSection', load);
        const tags = lazyProfileComponent('SidebarTagsSection', load);

        expect(about.displayName).toBe('SidebarAboutSection');
        expect(tags.displayName).toBe('SidebarTagsSection');
        expect(about.displayName).not.toBe(tags.displayName);
    });
});

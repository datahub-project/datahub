import { describe, expect, it } from 'vitest';

import { INITIAL_NAMESPACES, NAMESPACES } from '@src/i18n/namespaces';

describe('i18n bootstrap namespaces', () => {
    it('does not eagerly register namespaces for init-time loading', () => {
        expect(INITIAL_NAMESPACES).toEqual([]);
    });

    it('still exports the full namespace registry for tooling and parity checks', () => {
        expect(NAMESPACES.length).toBeGreaterThan(0);
        expect(NAMESPACES).toContain('common.actions');
        expect(NAMESPACES).toContain('home.v3');
    });
});

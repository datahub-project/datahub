import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import {
    OVERSCAN_AUTO_THRESHOLD,
    VIRT_AUTO_THRESHOLD,
    getLineagePerfFlags,
    resolveTriMode,
    resolveVirtEnabled,
} from '@app/lineageV2/perfFlags';

const STORAGE_KEY = 'datahub.lineagePerfFlags';

describe('resolveVirtEnabled', () => {
    it('honours explicit on/off regardless of node count', () => {
        expect(resolveVirtEnabled('on', 0)).toBe(true);
        expect(resolveVirtEnabled('on', VIRT_AUTO_THRESHOLD + 1000)).toBe(true);
        expect(resolveVirtEnabled('off', 0)).toBe(false);
        expect(resolveVirtEnabled('off', VIRT_AUTO_THRESHOLD + 1000)).toBe(false);
    });

    it('auto returns true only above the threshold', () => {
        expect(resolveVirtEnabled('auto', 0)).toBe(false);
        expect(resolveVirtEnabled('auto', VIRT_AUTO_THRESHOLD)).toBe(false);
        expect(resolveVirtEnabled('auto', VIRT_AUTO_THRESHOLD + 1)).toBe(true);
    });
});

describe('resolveTriMode', () => {
    it('on/off ignore the node count and threshold', () => {
        expect(resolveTriMode('on', 0, OVERSCAN_AUTO_THRESHOLD)).toBe(true);
        expect(resolveTriMode('off', 99999, OVERSCAN_AUTO_THRESHOLD)).toBe(false);
    });

    it('auto flips at the per-flag threshold (overscan)', () => {
        expect(resolveTriMode('auto', OVERSCAN_AUTO_THRESHOLD, OVERSCAN_AUTO_THRESHOLD)).toBe(false);
        expect(resolveTriMode('auto', OVERSCAN_AUTO_THRESHOLD + 1, OVERSCAN_AUTO_THRESHOLD)).toBe(true);
    });

    // Overscan kicks in well above virt so graphs just past VIRT_AUTO_THRESHOLD
    // don't pay the wider viewport bounds calculation.
    it('overscan threshold is higher than the virt threshold', () => {
        expect(OVERSCAN_AUTO_THRESHOLD).toBeGreaterThan(VIRT_AUTO_THRESHOLD);
    });
});

describe('getLineagePerfFlags', () => {
    const originalSearch = window.location.search;

    beforeEach(() => {
        window.localStorage.clear();
        Object.defineProperty(window, 'location', { value: { search: '' }, writable: true });
    });

    afterEach(() => {
        Object.defineProperty(window, 'location', {
            value: { search: originalSearch },
            writable: true,
        });
        vi.unstubAllGlobals();
    });

    // Overscan stays `'off'` by default — measured pan cost outweighs the
    // pop-in win at synthetic-100 scale.
    it('falls back to defaults when no source provides a value', () => {
        const flags = getLineagePerfFlags();
        expect(flags.virtMode).toBe('auto');
        expect(flags.overscanMode).toBe('off');
    });

    describe('server feature flag defaults', () => {
        it('virtEnabled=false flips the virt default to "off"', () => {
            const flags = getLineagePerfFlags({ virtEnabled: false, overscanEnabled: false });
            expect(flags.virtMode).toBe('off');
            expect(flags.overscanMode).toBe('off');
        });

        it('overscanEnabled=true flips the overscan default to "auto"', () => {
            const flags = getLineagePerfFlags({ virtEnabled: true, overscanEnabled: true });
            expect(flags.virtMode).toBe('auto');
            expect(flags.overscanMode).toBe('auto');
        });

        it('URL overrides win over server flag defaults', () => {
            Object.defineProperty(window, 'location', {
                value: { search: '?lineagePerf=virt-on,overscan-off' },
                writable: true,
            });
            const flags = getLineagePerfFlags({ virtEnabled: false, overscanEnabled: true });
            expect(flags.virtMode).toBe('on');
            expect(flags.overscanMode).toBe('off');
        });

        it('localStorage overrides win over server flag defaults', () => {
            window.localStorage.setItem(STORAGE_KEY, 'virt-on');
            const flags = getLineagePerfFlags({ virtEnabled: false });
            expect(flags.virtMode).toBe('on');
        });

        // Frequency-of-use case: server flag flips overscan on by default but
        // an operator pins it off mid-session via URL for an investigation.
        it('honours partial URL patches against server-driven defaults', () => {
            Object.defineProperty(window, 'location', {
                value: { search: '?lineagePerf=overscan-off' },
                writable: true,
            });
            const flags = getLineagePerfFlags({ virtEnabled: true, overscanEnabled: true });
            expect(flags.virtMode).toBe('auto');
            expect(flags.overscanMode).toBe('off');
        });
    });

    it('parses the legacy `virt` token as `virtMode: "on"`', () => {
        window.localStorage.setItem(STORAGE_KEY, 'virt');
        expect(getLineagePerfFlags().virtMode).toBe('on');
    });

    it.each([
        ['virt-on', 'on'],
        ['virt-off', 'off'],
        ['virt-auto', 'auto'],
    ] as const)('parses the `%s` token as `virtMode: "%s"`', (token, mode) => {
        window.localStorage.setItem(STORAGE_KEY, token);
        expect(getLineagePerfFlags().virtMode).toBe(mode);
    });

    // Old harness strings often carried `defer-minimap` / `no-transition`
    // tokens — they're silently ignored (rather than throwing) so saved
    // localStorage / URL values from earlier branches stay functional.
    it('silently drops removed flag tokens while honouring the rest', () => {
        window.localStorage.setItem(STORAGE_KEY, 'virt defer-minimap no-transition-auto');
        const flags = getLineagePerfFlags();
        expect(flags.virtMode).toBe('on');
        // Unknown / removed tokens must not contaminate other modes.
        expect(flags.overscanMode).toBe('off');
    });

    it.each([
        ['overscan', 'on'],
        ['overscan-on', 'on'],
        ['overscan-off', 'off'],
        ['overscan-auto', 'auto'],
    ] as const)('parses `%s` as `overscanMode: "%s"`', (token, mode) => {
        window.localStorage.setItem(STORAGE_KEY, token);
        expect(getLineagePerfFlags().overscanMode).toBe(mode);
    });

    it('URL params take precedence over localStorage', () => {
        window.localStorage.setItem(STORAGE_KEY, 'virt-off');
        Object.defineProperty(window, 'location', {
            value: { search: '?lineagePerf=virt-on' },
            writable: true,
        });
        expect(getLineagePerfFlags().virtMode).toBe('on');
    });

    it('ignores unknown tokens, leaving defaults intact', () => {
        window.localStorage.setItem(STORAGE_KEY, 'definitely-not-a-flag');
        expect(getLineagePerfFlags().virtMode).toBe('auto');
    });
});

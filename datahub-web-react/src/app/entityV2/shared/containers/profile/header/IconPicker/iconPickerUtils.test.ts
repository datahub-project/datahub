import { describe, expect, it } from 'vitest';

import {
    buildEffectiveDomainIconList,
    computeIconGridLayout,
    filterDomainIconsBySearch,
} from '@app/entityV2/shared/containers/profile/header/IconPicker/iconPickerUtils';

// The fixture library uses real Phosphor icon names so `hasLazyIcon` inside
// `buildEffectiveDomainIconList` treats them as loadable. Bogus names elsewhere in the
// tests are used to verify the guard against unknown icons.
const LIBRARY: readonly string[] = ['Rocket', 'Barn', 'UserCircle'];
const CURATED: Record<string, unknown> = { Rocket: {}, Barn: {}, UserCircle: {} };

describe('buildEffectiveDomainIconList', () => {
    it('returns the curated library unchanged when no pinned icons are provided', () => {
        expect(buildEffectiveDomainIconList(LIBRARY, CURATED)).toBe(LIBRARY);
        expect(buildEffectiveDomainIconList(LIBRARY, CURATED, [])).toBe(LIBRARY);
    });

    // Guards against duplicate cells for a pinned icon that happens to already be curated
    // — the customer's icon is visible either way, so we don't want it appearing twice.
    it('does not duplicate a pinned icon that is already in the curated set', () => {
        expect(buildEffectiveDomainIconList(LIBRARY, CURATED, ['Rocket'])).toBe(LIBRARY);
    });

    // Prevents rendering an AppWindow fallback cell for an unknown / empty name. The
    // rest of the library still renders behind it.
    it('ignores pinned names that are not real Phosphor icons', () => {
        expect(buildEffectiveDomainIconList(LIBRARY, CURATED, ['SomeUnknownIcon', ''])).toBe(LIBRARY);
    });

    it('prepends a valid pinned icon that is outside the curated set', () => {
        // `Wine` is a real Phosphor icon (loadable) but not in our fixture curated set.
        const result = buildEffectiveDomainIconList(LIBRARY, CURATED, ['Wine']);
        expect(result).toEqual(['Wine', 'Rocket', 'Barn', 'UserCircle']);
    });
});

describe('filterDomainIconsBySearch', () => {
    const icons: readonly string[] = ['Rocket', 'Barn', 'UserCircle', 'ShoppingCart'];

    it('returns the input unchanged for an empty or whitespace-only term', () => {
        expect(filterDomainIconsBySearch(icons, '')).toBe(icons);
        expect(filterDomainIconsBySearch(icons, '   ')).toBe(icons);
    });

    it('performs a case-insensitive substring match', () => {
        expect(filterDomainIconsBySearch(icons, 'user')).toEqual(['UserCircle']);
        expect(filterDomainIconsBySearch(icons, 'CART')).toEqual(['ShoppingCart']);
    });

    it('trims surrounding whitespace on the term before matching', () => {
        expect(filterDomainIconsBySearch(icons, '  rocket  ')).toEqual(['Rocket']);
    });

    it('returns an empty list when nothing matches', () => {
        expect(filterDomainIconsBySearch(icons, 'zzz')).toEqual([]);
    });
});

describe('computeIconGridLayout', () => {
    it('falls back to a single column of minCellSize when the container has not been measured', () => {
        expect(computeIconGridLayout(0, 56)).toEqual({ columnCount: 1, cellSize: 56 });
    });

    it('packs as many columns of at least minCellSize as fit', () => {
        expect(computeIconGridLayout(560, 56)).toEqual({ columnCount: 10, cellSize: 56 });
        expect(computeIconGridLayout(300, 56)).toEqual({ columnCount: 5, cellSize: 60 });
    });

    // Never yields zero columns even when the container is narrower than one cell —
    // an empty grid would leave the picker looking broken instead of just cramped.
    it('never returns fewer than 1 column', () => {
        expect(computeIconGridLayout(30, 56).columnCount).toBe(1);
    });

    it('uses the default minimum cell size when not provided', () => {
        expect(computeIconGridLayout(224)).toEqual({ columnCount: 4, cellSize: 56 });
    });
});

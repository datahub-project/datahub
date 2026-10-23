import { describe, expect, it } from 'vitest';

import { DEFAULT_PHOSPHOR_ICON, resolvePhosphorIconName } from '@app/sharedV2/icons/materialToPhosphorIcon';

describe('resolvePhosphorIconName', () => {
    it('returns null for empty names', () => {
        expect(resolvePhosphorIconName(null)).toBeNull();
        expect(resolvePhosphorIconName(undefined)).toBeNull();
        expect(resolvePhosphorIconName('')).toBeNull();
        expect(resolvePhosphorIconName('   ')).toBeNull();
    });

    it('maps legacy Material icon names to Phosphor equivalents', () => {
        expect(resolvePhosphorIconName('AccountCircle')).toBe('UserCircle');
        expect(resolvePhosphorIconName('AccountCircleOutlined')).toBe('UserCircle');
        expect(resolvePhosphorIconName('Home')).toBe('House');
        expect(resolvePhosphorIconName('Search')).toBe('MagnifyingGlass');
        expect(resolvePhosphorIconName('Wifi')).toBe('WifiHigh');
        expect(resolvePhosphorIconName('Undo')).toBe('ArrowUUpLeft');
        expect(resolvePhosphorIconName('Domain')).toBe('Buildings');
        expect(resolvePhosphorIconName('Webhook')).toBe('WebhooksLogo');
        expect(resolvePhosphorIconName('Fullscreen')).toBe('CornersOut');
    });

    it('passes through known Phosphor names when library is Material/missing', () => {
        // Domains often keep iconLibrary=MATERIAL after a Phosphor pick; still render the glyph.
        expect(resolvePhosphorIconName('UserCircle')).toBe('UserCircle');
        expect(resolvePhosphorIconName('Shapes')).toBe('Shapes');
        expect(resolvePhosphorIconName('Buildings')).toBe('Buildings');
    });

    it('falls back to the default for unmapped Material / legacy names', () => {
        expect(resolvePhosphorIconName('TotallyFakeIconOutlined')).toBe(DEFAULT_PHOSPHOR_ICON);
        expect(resolvePhosphorIconName('TotallyFakeIcon')).toBe(DEFAULT_PHOSPHOR_ICON);
    });

    it('keeps ChangeHistory mapped to ClockCounterClockwise', () => {
        expect(resolvePhosphorIconName('ChangeHistory')).toBe('ClockCounterClockwise');
        expect(resolvePhosphorIconName('ChangeHistoryOutlined')).toBe('ClockCounterClockwise');
    });
});

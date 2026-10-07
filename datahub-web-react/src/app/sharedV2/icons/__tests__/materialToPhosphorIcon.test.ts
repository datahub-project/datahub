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

    it('passes through Phosphor names unchanged', () => {
        expect(resolvePhosphorIconName('UserCircle')).toBe('UserCircle');
        expect(resolvePhosphorIconName('Shapes')).toBe('Shapes');
        expect(resolvePhosphorIconName('SealCheck')).toBe('SealCheck');
    });

    it('falls back to the default for Material-style names without a synonym', () => {
        expect(resolvePhosphorIconName('TotallyFakeIconOutlined')).toBe(DEFAULT_PHOSPHOR_ICON);
    });
});

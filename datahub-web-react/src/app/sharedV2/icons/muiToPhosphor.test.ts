import { describe, expect, it } from 'vitest';

import { resolveIconName } from '@app/sharedV2/icons/muiToPhosphor';

describe('resolveIconName', () => {
    it('returns empty string for null, undefined, or empty input', () => {
        expect(resolveIconName(null)).toBe('');
        expect(resolveIconName(undefined)).toBe('');
        expect(resolveIconName('')).toBe('');
    });

    // The pre-Phosphor default: every legacy domain has this icon unless the user
    // actively picked another. If this mapping breaks, virtually every existing
    // domain regresses to the letter-avatar fallback.
    it('maps the pre-migration default AccountCircle to UserCircle', () => {
        expect(resolveIconName('AccountCircle')).toBe('UserCircle');
    });

    it('maps common MUI names to their Phosphor equivalents', () => {
        expect(resolveIconName('Business')).toBe('Buildings');
        expect(resolveIconName('BarChart')).toBe('ChartBar');
        expect(resolveIconName('AttachMoney')).toBe('CurrencyDollar');
        expect(resolveIconName('School')).toBe('GraduationCap');
    });

    it('passes through names that are already Phosphor icons', () => {
        expect(resolveIconName('Rocket')).toBe('Rocket');
        expect(resolveIconName('Barn')).toBe('Barn');
    });

    // Guards against a rename in the map. An unknown name should never crash — it
    // passes through so the lazy loader can either resolve it as Phosphor or fall
    // back to AppWindow.
    it('passes through unknown names unchanged', () => {
        expect(resolveIconName('SomeRandomIconName')).toBe('SomeRandomIconName');
    });
});

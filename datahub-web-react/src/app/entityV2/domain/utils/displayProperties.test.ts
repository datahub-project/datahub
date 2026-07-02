import { describe, expect, it, vi } from 'vitest';

import {
    buildDomainDisplayInput,
    buildOptimisticDomainDisplayProperties,
    getDomainEditFieldChanges,
    resolveDomainEntityColor,
    resolveDomainIconDisplay,
} from '@app/entityV2/domain/utils/displayProperties';

import { IconLibrary } from '@types';

describe('buildDomainDisplayInput', () => {
    it('returns null when both fields are absent', () => {
        expect(buildDomainDisplayInput({})).toBeNull();
    });

    it('returns null when both fields are empty strings', () => {
        expect(buildDomainDisplayInput({ colorHex: '', iconName: '' })).toBeNull();
    });

    it('returns colorHex only when the icon is empty', () => {
        expect(buildDomainDisplayInput({ colorHex: '#002ad1' })).toEqual({ colorHex: '#002ad1' });
    });

    it('returns icon only when the color is empty', () => {
        expect(buildDomainDisplayInput({ iconName: 'Barn' })).toEqual({
            icon: { iconLibrary: IconLibrary.Material, name: 'Barn', style: 'Outlined' },
        });
    });

    it('returns both fields when both are set', () => {
        expect(buildDomainDisplayInput({ colorHex: '#ff0000', iconName: 'Rocket' })).toEqual({
            colorHex: '#ff0000',
            icon: { iconLibrary: IconLibrary.Material, name: 'Rocket', style: 'Outlined' },
        });
    });

    // Guards against a subtle bug where a real caller passes `iconName: undefined`
    // (no icon staged) alongside a genuine color pick — we should still emit the color.
    it('handles a color pick alongside an undefined icon', () => {
        expect(buildDomainDisplayInput({ colorHex: '#123456', iconName: undefined })).toEqual({
            colorHex: '#123456',
        });
    });
});

describe('resolveDomainEntityColor', () => {
    it('returns the saved colorHex when present', () => {
        const generateColor = vi.fn(() => '#palette');
        const color = resolveDomainEntityColor(
            { urn: 'urn:li:domain:x', displayProperties: { colorHex: '#002ad1' } },
            generateColor,
        );
        expect(color).toBe('#002ad1');
        expect(generateColor).not.toHaveBeenCalled();
    });

    it('falls back to generateColor(urn) when colorHex is missing', () => {
        const generateColor = vi.fn((urn: string) => `palette-for-${urn}`);
        const color = resolveDomainEntityColor({ urn: 'urn:li:domain:sales' }, generateColor);
        expect(color).toBe('palette-for-urn:li:domain:sales');
        expect(generateColor).toHaveBeenCalledWith('urn:li:domain:sales');
    });

    // A saved-but-empty colorHex is treated as "unset" — the palette fallback wins. This
    // matters because the create modal starts with a placeholder color and only writes
    // `colorHex` when the user actively picks something.
    it('treats empty-string colorHex as unset', () => {
        const generateColor = vi.fn(() => '#palette');
        expect(
            resolveDomainEntityColor({ urn: 'urn:li:domain:x', displayProperties: { colorHex: '' } }, generateColor),
        ).toBe('#palette');
    });

    it('handles a missing urn without throwing', () => {
        const generateColor = vi.fn(() => '#palette');
        expect(resolveDomainEntityColor({}, generateColor)).toBe('#palette');
        expect(generateColor).toHaveBeenCalledWith('');
    });
});

describe('resolveDomainIconDisplay', () => {
    it('returns no icon for null, undefined, or empty input', () => {
        expect(resolveDomainIconDisplay(null)).toEqual({ iconName: '', showIcon: false });
        expect(resolveDomainIconDisplay(undefined)).toEqual({ iconName: '', showIcon: false });
        expect(resolveDomainIconDisplay('')).toEqual({ iconName: '', showIcon: false });
    });

    // The pre-Phosphor default — nearly every legacy domain in an existing installation
    // stores this exact string, so a regression here silently regresses every domain
    // avatar to the letter fallback.
    it('maps the legacy AccountCircle name to UserCircle and shows it', () => {
        expect(resolveDomainIconDisplay('AccountCircle')).toEqual({
            iconName: 'UserCircle',
            showIcon: true,
        });
    });

    it('passes through post-migration Phosphor names unchanged', () => {
        expect(resolveDomainIconDisplay('Rocket')).toEqual({ iconName: 'Rocket', showIcon: true });
    });

    // Guards against rendering an AppWindow fallback — the letter avatar is a better
    // visual identity than a generic square for an unknown stored name.
    it('returns showIcon: false for names that resolve to nothing loadable', () => {
        expect(resolveDomainIconDisplay('SomeNonExistentIcon')).toEqual({
            iconName: 'SomeNonExistentIcon',
            showIcon: false,
        });
    });
});

describe('getDomainEditFieldChanges', () => {
    const initial = { name: 'Sales', colorHex: '#002ad1', displayedIconName: 'UserCircle' };

    it('detects no changes when everything matches', () => {
        expect(
            getDomainEditFieldChanges(initial, {
                trimmedName: 'Sales',
                colorHex: '#002ad1',
                iconName: 'UserCircle',
            }),
        ).toEqual({ nameChanged: false, colorChanged: false, iconChanged: false });
    });

    it('detects a name-only change', () => {
        expect(
            getDomainEditFieldChanges(initial, {
                trimmedName: 'Revenue',
                colorHex: '#002ad1',
                iconName: 'UserCircle',
            }),
        ).toEqual({ nameChanged: true, colorChanged: false, iconChanged: false });
    });

    it('detects a color-only change', () => {
        expect(
            getDomainEditFieldChanges(initial, {
                trimmedName: 'Sales',
                colorHex: '#ff0000',
                iconName: 'UserCircle',
            }),
        ).toEqual({ nameChanged: false, colorChanged: true, iconChanged: false });
    });

    // The critical MUI→Phosphor migration guard: if the stored icon was `AccountCircle`,
    // the displayed name is `UserCircle`, the picker's selected icon is `UserCircle`
    // (unchanged from what the user saw), then `iconChanged` MUST be false so we don't
    // silently rewrite the aspect on save.
    it('reports iconChanged: false when the staged pick matches the resolved-for-display name', () => {
        expect(
            getDomainEditFieldChanges(
                { name: 'Sales', colorHex: '', displayedIconName: 'UserCircle' },
                { trimmedName: 'Sales', colorHex: '', iconName: 'UserCircle' },
            ),
        ).toEqual({ nameChanged: false, colorChanged: false, iconChanged: false });
    });

    it('detects an icon change when the user actively picks a different one', () => {
        expect(
            getDomainEditFieldChanges(initial, {
                trimmedName: 'Sales',
                colorHex: '#002ad1',
                iconName: 'Rocket',
            }),
        ).toEqual({ nameChanged: false, colorChanged: false, iconChanged: true });
    });
});

describe('buildOptimisticDomainDisplayProperties', () => {
    it('returns null when neither field is set', () => {
        expect(buildOptimisticDomainDisplayProperties({})).toBeNull();
        expect(buildOptimisticDomainDisplayProperties({ colorHex: '', iconName: '' })).toBeNull();
    });

    // The sidebar renders `colorHex` directly — passing it as `null` (not `undefined`) matches
    // the Apollo fragment shape so the spread in `useManageDomains` doesn't erase the field.
    it('returns colorHex + null icon when only color is set', () => {
        expect(buildOptimisticDomainDisplayProperties({ colorHex: '#002ad1' })).toEqual({
            __typename: 'DisplayProperties',
            colorHex: '#002ad1',
            icon: null,
        });
    });

    it('returns null colorHex + icon when only icon is set', () => {
        expect(buildOptimisticDomainDisplayProperties({ iconName: 'Rocket' })).toEqual({
            __typename: 'DisplayProperties',
            colorHex: null,
            icon: {
                __typename: 'IconProperties',
                iconLibrary: IconLibrary.Material,
                name: 'Rocket',
                style: 'Outlined',
            },
        });
    });

    // Full-shape regression: the sidebar's `DomainColoredIcon` reads both fields off this
    // object, so any drift in the __typename tags or key names silently breaks optimistic
    // updates.
    it('returns the full DisplayProperties fragment shape when both fields are set', () => {
        expect(buildOptimisticDomainDisplayProperties({ colorHex: '#ff0000', iconName: 'Barn' })).toEqual({
            __typename: 'DisplayProperties',
            colorHex: '#ff0000',
            icon: {
                __typename: 'IconProperties',
                iconLibrary: IconLibrary.Material,
                name: 'Barn',
                style: 'Outlined',
            },
        });
    });
});

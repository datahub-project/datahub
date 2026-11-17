import { describe, expect, it } from 'vitest';

import { getColor, getThemedIconColor, getThemedTextColor } from '@components/theme/utils';

import themeV2 from '@conf/theme/themeV2';
import themeV2Dark from '@conf/theme/themeV2Dark';

const themes = [themeV2, themeV2Dark];

describe.each(themes)('themed color resolution for $id', (theme) => {
    it.each([
        ['gray', 'textSecondary'],
        ['primary', 'textBrand'],
        ['violet', 'textBrand'],
        ['red', 'textError'],
        ['green', 'textSuccess'],
        ['blue', 'textInformation'],
        ['yellow', 'textWarning'],
    ] as const)('maps Text color="%s" to %s', (color, token) => {
        expect(getThemedTextColor(color, undefined, theme)).toBe(theme.colors[token]);
    });

    it.each(['gray', 'primary', 'violet', 'red', 'green', 'blue', 'yellow'] as const)(
        'keeps an explicit Text colorLevel for color="%s"',
        (color) => {
            expect(getThemedTextColor(color, 500, theme)).toBe(getColor(color, 500, theme));
        },
    );

    it.each([
        ['gray', 'icon'],
        ['primary', 'iconBrand'],
        ['violet', 'iconBrand'],
        ['red', 'iconError'],
        ['green', 'iconSuccess'],
        ['blue', 'iconInformation'],
        ['yellow', 'iconWarning'],
    ] as const)('maps Icon color="%s" to %s', (color, token) => {
        expect(getThemedIconColor(color, undefined, theme)).toBe(theme.colors[token]);
    });

    it.each(['gray', 'primary', 'violet', 'red', 'green', 'blue', 'yellow'] as const)(
        'keeps an explicit Icon colorLevel for color="%s"',
        (color) => {
            expect(getThemedIconColor(color, 500, theme)).toBe(getColor(color, 500, theme));
        },
    );

    it('continues to resolve explicit semantic tokens and inherited color', () => {
        expect(getThemedTextColor('textTertiary', undefined, theme)).toBe(theme.colors.textTertiary);
        expect(getThemedIconColor('iconDisabled', undefined, theme)).toBe(theme.colors.iconDisabled);
        expect(getThemedTextColor('inherit', undefined, theme)).toBe('inherit');
    });
});

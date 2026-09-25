import { create } from '@storybook/theming';

import { typography } from '../src/alchemy-components/theme';
import type ColorTheme from '../src/conf/theme/colorThemes/types';
import themes from '../src/conf/theme/themes';

/**
 * Builds a Storybook docs theme from the app's semantic color tokens.
 *
 * Autodocs pages are rendered by Storybook, not by us, and it colors its own
 * prose, tables and TOC through emotion classes with generated names. Handing
 * it a theme is the supported way to recolor them — overriding the output with
 * CSS would mean targeting hashes that change on every Storybook upgrade.
 *
 * @param base - Which built-in palette Storybook falls back to for anything not mapped here
 * @param colors - Semantic color tokens for the matching app theme
 * @returns A Storybook theme for use as `parameters.docs.container` theme
 */
function buildDocsTheme(base: 'light' | 'dark', colors: ColorTheme) {
    return create({
        base,
        fontBase: typography.fonts.body,
        fontCode: 'monospace',

        colorPrimary: colors.textBrand,
        colorSecondary: colors.textBrand,

        appBg: colors.bg,
        appContentBg: colors.bg,
        appPreviewBg: colors.bg,
        appBorderColor: colors.border,
        appBorderRadius: 4,

        textColor: colors.text,
        textMutedColor: colors.textSecondary,

        barBg: colors.bg,
        barTextColor: colors.text,
        barSelectedColor: colors.textBrand,
        barHoverColor: colors.textBrand,

        inputBg: colors.bgSurface,
        inputBorder: colors.border,
        inputTextColor: colors.text,
        inputBorderRadius: 4,
    });
}

export const lightDocsTheme = buildDocsTheme('light', themes.themeV2.colors);
export const darkDocsTheme = buildDocsTheme('dark', themes.themeV2Dark.colors);

import { create } from '@storybook/theming';

import theme, { typography } from '../src/alchemy-components/theme';
import brandImage from './storybook-logo.svg';

// The dark palette is inlined rather than imported from
// `src/conf/theme/colorThemes/dark`, because that module resolves through the
// app's `@conf/*` path aliases, which the manager bundle does not configure.
// These mirror the dark theme's core tokens: bg (gray1100), bgSurface
// (gray1200), border (gray900), text (gray300), textSecondary (gray400),
// textBrand (violet300).
const darkPalette = {
    bg: '#24232B',
    bgSurface: '#1C1B23',
    border: '#3C3B44',
    text: '#D1D1D9',
    textSecondary: '#BBBBC5',
    textBrand: '#B0A7EA',
    inverseText: '#FFFFFF',
};

const brand = {
    brandTitle: 'DataHub Design System',
    brandUrl: '/?path=/docs/',
    brandImage,
    brandTarget: '_self',

    fontBase: typography.fonts.body,
    fontCode: 'monospace',

    appBorderRadius: 4,
    inputBorderRadius: 4,
    gridCellSize: 6,
};

export const lightTheme = create({
    ...brand,
    base: 'light',

    colorPrimary: theme.semanticTokens.colors.secondary,
    colorSecondary: theme.semanticTokens.colors.secondary,

    // UI
    appBg: theme.semanticTokens.colors['body-bg'],
    appContentBg: theme.semanticTokens.colors['body-bg'],
    appPreviewBg: theme.semanticTokens.colors['body-bg'],
    appBorderColor: theme.semanticTokens.colors['border-color'],

    // Text colors
    textColor: theme.semanticTokens.colors['body-text'],
    textInverseColor: theme.semanticTokens.colors['inverse-text'],
    textMutedColor: theme.semanticTokens.colors['subtle-text'],

    // Toolbar default and active colors. These are all text colors — pointing the
    // selected/hover ones at a background token rendered active items near-invisible.
    barTextColor: theme.semanticTokens.colors['body-text'],
    barSelectedColor: theme.semanticTokens.colors['body-text'],
    barHoverColor: theme.semanticTokens.colors['body-text'],
    barBg: theme.semanticTokens.colors['body-bg'],

    // Form colors
    inputBg: theme.semanticTokens.colors['body-bg'],
    inputBorder: theme.semanticTokens.colors['border-color'],
    inputTextColor: theme.semanticTokens.colors['body-text'],
});

export const darkTheme = create({
    ...brand,
    base: 'dark',

    colorPrimary: darkPalette.textBrand,
    colorSecondary: darkPalette.textBrand,

    // UI
    appBg: darkPalette.bgSurface,
    appContentBg: darkPalette.bg,
    appPreviewBg: darkPalette.bg,
    appBorderColor: darkPalette.border,

    // Text colors
    textColor: darkPalette.text,
    textInverseColor: darkPalette.inverseText,
    textMutedColor: darkPalette.textSecondary,

    // Toolbar
    barTextColor: darkPalette.text,
    barSelectedColor: darkPalette.textBrand,
    barHoverColor: darkPalette.textBrand,
    barBg: darkPalette.bgSurface,

    // Form colors
    inputBg: darkPalette.bgSurface,
    inputBorder: darkPalette.border,
    inputTextColor: darkPalette.text,
});

export default lightTheme;

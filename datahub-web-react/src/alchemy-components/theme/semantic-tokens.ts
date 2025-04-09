import { foundations } from './foundations';

const { colors } = foundations;

// Define color modes (light and dark)
const colorModes = {
    light: {
        // Text colors
        'text-primary': colors.gray[600],
        'text-secondary': colors.gray[1700],
        'text-tertiary': colors.gray[1800],
        'placeholder-text': colors.gray[1800],
        'inverse-text': colors.white,
        'disabled-text': colors.gray[1800],

        // Background colors
        bg: colors.white,
        'surface-bg': colors.gray[1500],
        'subtle-bg': colors.gray[100],
        'nav-bg': colors.gray[1600],

        // Border colors
        border: colors.gray[100],
        'border-subtle': colors.gray[1400],

        // Icon colors
        'icon-color': colors.gray[1800],

        // Component specific
        'placeholder-color': colors.gray[1800],

        // Brand/semantic colors
        primary: colors.violet[500],
        secondary: colors.blue[500],
        error: colors.red[500],
        success: colors.green[500],
        warning: colors.yellow[500],
        info: colors.blue[500],

        // Brand color variations
        'primary-light': colors.violet[0],
        'primary-dark': colors.violet[700],
        'secondary-light': colors.blue[0],
        'secondary-dark': colors.blue[700],
        'error-light': colors.red[0],
        'error-dark': colors.red[700],
        'success-light': colors.green[0],
        'success-dark': colors.green[700],
        'warning-light': colors.yellow[0],
        'warning-dark': colors.yellow[700],
        'info-light': colors.blue[0],
        'info-dark': colors.blue[700],
    },
    dark: {
        // Text colors
        'text-primary': colors.white,
        'text-secondary': colors.gray[300],
        'text-tertiary': colors.gray[400],
        'placeholder-text': colors.gray[400],
        'inverse-text': colors.gray[800],
        'disabled-text': colors.gray[500],

        // Background colors
        bg: colors.gray[800],
        'surface-bg': colors.gray[700],
        'subtle-bg': colors.gray[600],
        'nav-bg': colors.gray[700],

        // Border colors
        border: colors.gray[600],
        'border-subtle': colors.gray[500],

        // Icon colors
        'icon-color': colors.gray[300],

        // Component specific
        'placeholder-color': colors.gray[500],

        // Brand/semantic colors
        primary: colors.violet[300],
        secondary: colors.blue[300],
        error: colors.red[300],
        success: colors.green[300],
        warning: colors.yellow[300],
        info: colors.blue[300],

        // Brand color variations
        'primary-light': colors.violet[400],
        'primary-dark': colors.violet[200],
        'secondary-light': colors.blue[400],
        'secondary-dark': colors.blue[200],
        'error-light': colors.red[400],
        'error-dark': colors.red[200],
        'success-light': colors.green[400],
        'success-dark': colors.green[200],
        'warning-light': colors.yellow[400],
        'warning-dark': colors.yellow[200],
        'info-light': colors.blue[400],
        'info-dark': colors.blue[200],
    },
};

// Type for semantic color tokens
export type SemanticColorTokens = typeof colorModes.light;
export type SemanticTokens = { colors: SemanticColorTokens };

// This is the default export that will be used by the theme
export const semanticTokens = {
    colors: colorModes.light,
};

// Export both modes to enable theme switching
export const colorTokens = colorModes;

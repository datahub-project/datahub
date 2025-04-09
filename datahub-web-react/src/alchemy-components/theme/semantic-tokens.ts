import { foundations } from './foundations';

const { colors } = foundations;

// Define color modes (light and dark)
const colorModes = {
    light: {
        // Text colors
        'text-primary': colors.gray[600],
        'text-secondary': colors.gray[1700],
        'text-tertiary': colors.gray[1800],
        'text-brand': colors.violet[600],
        'text-placeholder': colors.gray[1800],
        'text-inverse': colors.white,
        'text-disabled': colors.gray[1800],

        'body-text': colors.gray[600], // deprecated please use text-primary
        'subtle-text': colors.gray[1700], // deprecated please use text-secondary
        'inverse-text': colors.white, // deprecated please use text-inverse

        // Background colors
        'bg-primary': colors.white,
        'bg-surface': colors.gray[1500],
        'bg-subtle': colors.gray[100],
        'bg-nav': colors.gray[1600],

        'body-bg': colors.white, // deprecated please use bg-primary

        // Border colors
        'border-color': colors.gray[100],
        'border-subtle': colors.gray[1400],

        // Icon colors
        'icon-color': colors.gray[1800],

        // Component specific
        'placeholder-color': colors.gray[1800], // deprecated please use text-placeholder

        // Brand/semantic colors
        primary: colors.violet[600],
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
        'text-brand': colors.violet[300],
        'text-placeholder': colors.gray[400],
        'text-inverse': colors.gray[800],
        'text-disabled': colors.gray[500],

        'body-text': colors.white, // deprecated please use text-primary
        'subtle-text': colors.gray[300], // deprecated please use text-secondary
        'inverse-text': colors.gray[800], // deprecated please use text-inverse

        // Background colors
        'bg-primary': colors.gray[2100],
        'bg-surface': colors.gray[2000],
        'bg-subtle': colors.gray[600],
        'bg-nav': colors.gray[700],

        'body-bg': colors.gray[2000], // deprecated please use bg-primary
        // Border colors
        'border-color': colors.gray[600],
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

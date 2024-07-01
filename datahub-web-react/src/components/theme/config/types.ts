/*
	Theme type definitions that can be used anywhere in the app
*/

// General types
export type SizeOptions = 'sm' | 'md' | 'lg' | 'xl';

// Color types
export interface Color {
    25: string;
    50: string;
    100: string;
    200: string;
    300: string;
    400: string;
    500: string;
    600: string;
    700: string;
    800: string;
    900: string;
    1000: string;
}
export type ColorOptions = 'white' | 'black' | 'violet' | 'green' | 'red' | 'blue' | 'gray';
export type ColorShadeOptions = 'light' | 'default' | 'dark';

// Typography types
export type FontSizeOptions = 'xs' | 'sm' | 'md' | 'lg' | 'xl' | '2xl' | '3xl' | '4xl';
export type FontWeightOptions = 'light' | 'normal' | 'bold' | 'black';
export type FontVariantOptions = 'heading' | 'subheading' | 'body' | 'caption';
export type FontColorOptions = 'inherit' | ColorOptions;

// Border radius types
export type BorderRadiusOptions = 'none' | 'sm' | 'md' | 'lg' | 'xl' | 'full';

// Box shadow types
export type BoxShadowOptions = 'xs' | 'sm' | 'md' | 'lg' | 'xl' | '2xl' | 'inner' | 'outline' | 'none';

// Spacing types
export type SpacingOptions = 'none' | 'sm' | 'md' | 'lg' | 'xl' | '3xl' | '4xl';

// Transform types
export type RotationOptions = '0' | '90' | '180' | '270';

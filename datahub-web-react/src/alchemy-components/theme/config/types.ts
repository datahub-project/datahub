// General types
export type SizeOptions = 'xs' | 'sm' | 'md' | 'lg' | 'xl';

// Color types
export interface Color {
    100: string;
    200: string;
    300: string;
    400: string;
    500: string;
    600: string;
    700: string;
    800: string;
    900: string;
}
export type ColorOptions = 'white' | 'black' | 'violet' | 'green' | 'red' | 'blue' | 'gray' | 'yellow';
export type MiscColorOptions = 'transparent' | 'current' | 'inherit';
export type ColorShadeOptions = 'light' | 'default' | 'dark';

// Typography types
export type FontSizeOptions = 'xs' | 'sm' | 'md' | 'lg' | 'xl' | '2xl' | '3xl' | '4xl';
export type FontWeightOptions = 'normal' | 'medium' | 'semiBold' | 'bold';
export type FontColorOptions = MiscColorOptions | ColorOptions;

// Border radius types
export type BorderRadiusOptions = 'none' | 'sm' | 'md' | 'lg' | 'xl' | 'full';

// Box shadow types
export type BoxShadowOptions = 'xs' | 'sm' | 'md' | 'lg' | 'xl' | '2xl' | 'inner' | 'outline' | 'dropdown' | 'none';

// Spacing types
export type SpacingOptions = 'none' | 'sm' | 'md' | 'lg' | 'xl' | '3xl' | '4xl';

// Transform types
export type RotationOptions = '0' | '90' | '180' | '270';

// Variant types
export type VariantOptions = 'filled' | 'outline';

// Alignment types
export type AlignmentOptions = 'left' | 'right' | 'center' | 'justify';

// Avatar Size options
export type AvatarSizeOptions = 'sm' | 'md' | 'lg' | 'default';

// Icon Alignment types
export type IconAlignmentOptions = 'horizontal' | 'vertical';

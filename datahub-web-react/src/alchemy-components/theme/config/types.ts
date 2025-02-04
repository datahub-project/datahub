// General types
export enum SizeValues {
    xs = 'xs',
    sm = 'sm',
    md = 'md',
    lg = 'lg',
    xl = 'xl',
}
export type SizeOptions = keyof typeof SizeValues;
export function getSizeName(size: SizeOptions): string {
    switch (size) {
        case SizeValues.xs:
            return 'xs';
        case SizeValues.sm:
            return 'small';
        case SizeValues.md:
            return 'medium';
        case SizeValues.lg:
            return 'large';
        case SizeValues.xl:
            return 'extra large';
        default:
            return '';
    }
}

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
    1000: string;
    1100: string;
    1200: string;
    1300: string;
    1400: string;
    1500: string;
    1600: string;
    1700: string;
    1800: string;
    1900: string;
}

export enum ColorValues {
    white = 'white',
    black = 'black',
    violet = 'violet',
    green = 'green',
    red = 'red',
    blue = 'blue',
    yellow = 'yellow',
    gray = 'gray',
}

export type ColorOptions = keyof typeof ColorValues;

export type MiscColorOptions = 'transparent' | 'current' | 'inherit';
export type ColorShadeOptions = 'light' | 'default' | 'dark';

// Typography types
export enum FontSizeValues {
    xs = 'xs',
    sm = 'sm',
    md = 'md',
    lg = 'lg',
    xl = 'xl',
    '2xl' = '2xl',
    '3xl' = '3xl',
    '4xl' = '4xl',
}
export type FontSizeOptions = keyof typeof FontSizeValues;
export type FontWeightOptions = 'normal' | 'medium' | 'semiBold' | 'bold';
export type FontColorOptions = MiscColorOptions | ColorOptions;

export type BorderRadiusOptions = 'none' | 'sm' | 'md' | 'lg' | 'xl' | 'full';

export type BoxShadowOptions = 'xs' | 'sm' | 'md' | 'lg' | 'xl' | '2xl' | 'inner' | 'outline' | 'dropdown' | 'none';

export type SpacingOptions = 'none' | 'sm' | 'md' | 'lg' | 'xl' | '2xl' | '3xl' | '4xl';

export type RotationOptions = '0' | '90' | '180' | '270';

export enum PillVariantValues {
    filled = 'filled',
    outline = 'outline',
    version = 'version',
}

export type PillVariantOptions = keyof typeof PillVariantValues;

export type AlignmentOptions = 'left' | 'right' | 'center' | 'justify';

export type AvatarSizeOptions = 'sm' | 'md' | 'lg' | 'xl' | 'default';

export type IconAlignmentOptions = 'horizontal' | 'vertical';

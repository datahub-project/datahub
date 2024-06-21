/*
	Theme type definitions that can be used anywhere in the app
*/

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

export type ColorShade = 'light' | 'default' | 'dark';
export type ColorType = 'white' | 'black' | 'gray' | 'violet' | 'green' | 'red' | 'blue';

export type FontSize = 'sm' | 'md' | 'lg' | 'xl' | 'xxl' | 'xxxl' | 'xxxxl';
export type FontWeight = 'light' | 'normal' | 'bold';
export type FontVariant = 'heading' | 'subheading' | 'body' | 'caption';

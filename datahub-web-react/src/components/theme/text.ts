/*
	Font values for the theme
*/

import type { FontSizeOptions, FontWeightOptions } from './config/types';

const size: Record<FontSizeOptions, string> = {
    xs: '10px',
    sm: '12px',
    md: '14px', // default body text size
    lg: '16px',
    xl: '18px',
    '2xl': '20px',
    '3xl': '22px',
    '4xl': '24px',
};

const weight: Record<FontWeightOptions, number> = {
    light: 300,
    normal: 500,
    bold: 600,
    black: 900,
};

const family = {
    default: 'Mulish',
};

const lineHeight = {
    xs: '16px',
    sm: '20px',
    md: '24px',
    lg: '28px',
    xl: '32px',
    '2xl': '36px',
    '3xl': '40px',
    '4xl': '44px',
};

export const text = {
    size,
    weight,
    family,
    lineHeight,
};

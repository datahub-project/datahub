/*
	Font values for the theme
*/

import type { FontSizeOptions, FontWeightOptions } from './config/types';

const size: Record<FontSizeOptions, string> = {
    xs: '10px',
    sm: '12px',
    md: '14px',
    lg: '16px',
    xl: '18px',
    '2xl': '20px',
    '3xl': '22px',
    '4xl': '24px',
};

const weight: Record<FontWeightOptions, number> = {
    light: 400,
    normal: 500,
    black: 700,
};

const family = {
    default: 'Mulish',
};

const lineHeight = {
    normal: 'normal',
};

export const text = {
    size,
    weight,
    family,
    lineHeight,
};

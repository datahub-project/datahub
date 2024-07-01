/*
	Theme Utils that can be used anywhere in the app
*/

import { colors } from './colors';
import { text } from './text';
import { tokens } from './tokens';
import { transform } from './transform';

import { FontVariantOptions, FontSizeOptions, FontColorOptions, RotationOptions } from './config/types';

import { DEFAULT_VALUE } from './config/constants';

/*
	Get the font values for a given size and variant
	@param size - the size of the font
	@param variant - the variant of the font
*/
export const getFontValues = ({ size = 'md', variant = 'body' }: { size?: string; variant?: FontVariantOptions }) => {
    const { size: sizeValues } = text;
    const {
        text: { weight },
        colors: { text: textColors },
    } = tokens;

    // Calcuate the color based on the variant
    let color;

    switch (variant) {
        case 'heading':
            color = textColors.dark;
            break;
        case 'subheading':
        case 'caption':
            color = textColors.light;
            break;
        default:
            color = textColors.default;
            break;
    }

    // Return the font values
    return {
        size: sizeValues[size as FontSizeOptions],
        weight: weight[variant],
        family: text.family.default,
        lineHeight: text.lineHeight.normal,
        color,
    };
};

/*
	Get the font size value for a given size
	@param size - the size of the font
*/
export const getFontSize = (size?: FontSizeOptions) => {
    let sizeValue = size || '';
    if (!size) sizeValue = 'md';
    const { size: sizeValues } = text;
    return sizeValues[sizeValue];
};

/*
	Get the color value for a given color
	@param color - the color to get the value for
*/
export const getColorValue = (color?: FontColorOptions, value: number | string = DEFAULT_VALUE) => {
    if (!color) return colors.black;
    if (color === 'inherit') return 'inherit';
    if (color === 'white') return colors.white;
    if (color === 'black') return colors.black;
    const colorValue = colors[color];
    if (!colorValue) return colors.black;
    return colors[color][value];
};

/*
	Get the rotation transform value for a given rotation
	@param r - the rotation to get the transform value for
*/
export const getRotationTransform = (rotate?: RotationOptions) => {
    if (!rotate) return '';
    return transform.rotate[rotate || '0'];
};

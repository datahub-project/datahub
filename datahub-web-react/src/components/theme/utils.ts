/*
	Theme Utils that can be used anywhere in the app
*/

import { colors } from './colors';
import { text } from './text';
import { tokens } from './tokens';
import { transform } from './transform';

import {
    FontVariantOptions,
    FontSizeOptions,
    FontColorOptions,
    ColorOptions,
    ColorShadeOptions,
    RotationOptions,
} from './config/types';

import {
    LIGHT_VALUE,
    LIGHT_HOVER_VALUE,
    DEFAULT_VALUE,
    DEFAULT_HOVER_VALUE,
    DARK_VALUE,
    DARK_HOVER_VALUE,
} from './config/constants';

/*
	Get the color values for a given color and shade
	@param color - the color to get the values for
	@param shade - the shade of the color to get the values for
*/
export const getColorValues = ({ color, shade = 'default' }: { color: ColorOptions; shade?: ColorShadeOptions }) => {
    // If the color is not provided, throw an error
    if (!color) throw new Error('Color is required');

    // If the color is white, return the white color
    if (color === 'white') {
        return {
            default: colors.white,
            hover: colors.white,
        };
    }

    // If the color is black, return the black color
    if (color === 'black') {
        return {
            default: colors.black,
            hover: colors.black,
        };
    }

    // Get the color values for the color
    const colorValues = colors[color];

    // If the color does not exist in the theme, throw an error
    if (!colorValues) throw new Error(`Color ${color} does not exist in the theme`);

    // Get the value for the shade
    let shadeValue;
    let hoverValue;

    switch (shade) {
        case 'light':
            shadeValue = LIGHT_VALUE;
            hoverValue = LIGHT_HOVER_VALUE;
            break;
        case 'dark':
            shadeValue = DARK_VALUE;
            hoverValue = DARK_HOVER_VALUE;
            break;
        default:
            shadeValue = DEFAULT_VALUE;
            hoverValue = DEFAULT_HOVER_VALUE;
            break;
    }

    // Return the color values
    return {
        default: colorValues[shadeValue],
        hover: colorValues[hoverValue],
    };
};

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
export const getColorValue = (color?: FontColorOptions) => {
    if (!color) return colors.black;
    if (color === 'inherit') return 'inherit';
    if (color === 'white') return colors.white;
    if (color === 'black') return colors.black;
    return colors[color][DEFAULT_VALUE];
};

/*
	Get the rotation transform value for a given rotation
	@param r - the rotation to get the transform value for
*/
export const getRotationTransform = (rotate?: RotationOptions) => {
    if (!rotate) return '';
    return transform.rotate[rotate || '0'];
};

/*
	Theme Utils that can be used anywhere in the app
*/

import { FontSizeOptions, ColorOptions, MiscColorOptions, RotationOptions, DEFAULT_VALUE } from './config';
import { foundations } from './foundations';
import { semanticTokens } from './semantic-tokens';

const { colors, typography, transform } = foundations;
/*
	Get the color value for a given color
	Falls back to `color.black` if the color is not found
	@param color - the color to get the value for
*/
export const getColor = (color?: MiscColorOptions | ColorOptions, value: number | string = DEFAULT_VALUE) => {
    if (!color) return colors.black;
    if (color === 'inherit' || color === 'transparent' || color === 'current') return colors;
    if (color === 'white') return colors.white;
    if (color === 'black') return colors.black;
    const colorValue = colors[color];
    if (!colorValue) return colors.black;
    return colors[color][value];
};

/*
	Get the font size value for a given size
	@param size - the size of the font
*/
export const getFontSize = (size?: FontSizeOptions) => {
    let sizeValue = size || '';
    if (!size) sizeValue = 'md';
    return typography.fontSizes[sizeValue];
};

/*
	Get the rotation transform value for a given rotation
	@param r - the rotation to get the transform value for
*/
export const getRotationTransform = (rotate?: RotationOptions) => {
    if (!rotate) return '';
    return transform.rotate[rotate || '0'];
};

/**
 * Get the status color depending on the flags that are true
 * @param {string} [error] - Error definition, if any.
 * @param {boolean} [isSuccess] - Boolean flag indicating success.
 * @param {string} [warning] - Warning definition, if any.
 * @returns {string} - The status color based on the provided flags.
 */
export const getStatusColors = (isSuccess?: boolean, warning?: string, isInvalid?: boolean): string => {
    if (isInvalid) {
        return colors.red[600];
    }
    if (isSuccess) {
        return colors.green[600];
    }
    if (warning) {
        return colors.yellow[600];
    }
    return semanticTokens.colors['border-color'];
};

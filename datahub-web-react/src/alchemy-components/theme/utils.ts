/*
	Theme Utils that can be used anywhere in the app
*/
import ColorTheme from '@conf/theme/colorThemes/types';
import { Theme } from '@conf/theme/types';

import {
    ColorOptions,
    DEFAULT_VALUE,
    FontColorLevelOptions,
    FontColorOptions,
    FontSizeOptions,
    MiscColorOptions,
    RotationOptions,
} from './config';
import { foundations } from './foundations';

const { colors, typography, transform } = foundations;

const TEXT_COLOR_TOKENS: Partial<Record<ColorOptions, keyof ColorTheme>> = {
    gray: 'textSecondary',
    primary: 'textBrand',
    violet: 'textBrand',
    red: 'textError',
    green: 'textSuccess',
    blue: 'textInformation',
    yellow: 'textWarning',
};

const ICON_COLOR_TOKENS: Partial<Record<ColorOptions, keyof ColorTheme>> = {
    gray: 'icon',
    primary: 'iconBrand',
    violet: 'iconBrand',
    red: 'iconError',
    green: 'iconSuccess',
    blue: 'iconInformation',
    yellow: 'iconWarning',
};

/*
	Get the color value for a given color
	Falls back to `color.black` if the color is not found
	@param color - the color to get the value for
*/
export const getColor = (
    color?: MiscColorOptions | ColorOptions,
    value: number | string = DEFAULT_VALUE,
    theme?: Theme,
) => {
    let finalColors = colors;
    if (theme?.colors) {
        finalColors = { ...colors, ...theme.colors };
    }

    if (!color) return finalColors.black;
    if (color === 'inherit' || color === 'transparent') return color;
    if (color === 'current') return 'currentColor';
    if (color === 'white') return finalColors.white;
    if (color === 'black') return finalColors.black;
    const colorValue = finalColors[color];
    if (!colorValue) return finalColors.black;
    return finalColors[color][value];
};

const getThemedColor = (
    color: FontColorOptions | undefined,
    colorLevel: FontColorLevelOptions | undefined,
    theme: Theme | undefined,
    colorTokens: Partial<Record<ColorOptions, keyof ColorTheme>>,
): string => {
    if (color && theme?.colors) {
        const token = colorTokens[color as ColorOptions] ?? (color as keyof ColorTheme);
        const semanticColor = theme.colors[token];
        if (typeof semanticColor === 'string') return semanticColor;
    }

    return getColor(color as MiscColorOptions | ColorOptions, colorLevel, theme);
};

export const getThemedTextColor = (
    color?: FontColorOptions,
    colorLevel?: FontColorLevelOptions,
    theme?: Theme,
): string => getThemedColor(color, colorLevel, theme, TEXT_COLOR_TOKENS);

export const getThemedIconColor = (
    color?: FontColorOptions,
    colorLevel?: FontColorLevelOptions,
    theme?: Theme,
): string => getThemedColor(color, colorLevel, theme, ICON_COLOR_TOKENS);

/*
	Get the font size value for a given size
	@param size - the size of the font
*/
export const getFontSize = (size?: FontSizeOptions) => {
    if (size === 'inherit') return 'inherit';
    return typography.fontSizes[size || 'md'];
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
export const getStatusColors = (
    isSuccess?: boolean,
    warning?: string,
    isInvalid?: boolean,
    themeColors?: { borderError: string; borderSuccess: string; borderWarning: string; borderInput: string },
): string => {
    if (isInvalid) {
        return themeColors?.borderError ?? colors.red[600];
    }
    if (isSuccess) {
        return themeColors?.borderSuccess ?? colors.green[600];
    }
    if (warning) {
        return themeColors?.borderWarning ?? colors.yellow[600];
    }
    return themeColors?.borderInput ?? colors.gray[100];
};

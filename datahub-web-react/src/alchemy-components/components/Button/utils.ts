/*
 * Button Style Utilities
 */

import { typography, colors, shadows, radius, spacing } from '@components/theme';
import { ColorOptions, SizeOptions } from '@components/theme/config';
import { getColor, getFontSize } from '@components/theme/utils';
import { CSSObject } from 'styled-components';
import { ButtonStyleProps, ButtonVariant } from './types';

interface ColorStyles {
    bgColor: string;
    hoverBgColor: string;
    activeBgColor: string;
    disabledBgColor: string;
    borderColor: string;
    activeBorderColor: string;
    disabledBorderColor: string;
    textColor: string;
    disabledTextColor: string;
}

// Utility function to get color styles for button - does not generate CSS
const getButtonColorStyles = (variant: ButtonVariant, color: ColorOptions): ColorStyles => {
    const color500 = getColor(color, 500); // value of 500 shade
    const isViolet = color === 'violet';

    const base = {
        // Backgrounds
        bgColor: color500,
        hoverBgColor: getColor(color, 600),
        activeBgColor: getColor(color, 700),
        disabledBgColor: getColor('gray', 100),

        // Borders
        borderColor: color500,
        activeBorderColor: getColor(color, 300),
        disabledBorderColor: getColor('gray', 200),

        // Text
        textColor: colors.white,
        disabledTextColor: getColor('gray', 300),
    };

    // Specific color override for white
    if (color === 'white') {
        base.textColor = colors.black;
        base.disabledTextColor = getColor('gray', 500);
    }

    // Specific color override for gray
    if (color === 'gray') {
        base.textColor = getColor('gray', 500);
        base.bgColor = getColor('gray', 100);
        base.borderColor = getColor('gray', 100);

        base.hoverBgColor = getColor('gray', 100);
        base.activeBgColor = getColor('gray', 200);
    }

    // Override styles for outline variant
    if (variant === 'outline') {
        return {
            ...base,
            bgColor: colors.transparent,
            borderColor: color500,
            textColor: color500,

            hoverBgColor: isViolet ? getColor(color, 100) : getColor(color, 100),
            activeBgColor: isViolet ? getColor(color, 100) : getColor(color, 200),

            disabledBgColor: 'transparent',
        };
    }

    // Override styles for text variant
    if (variant === 'text') {
        return {
            ...base,
            textColor: color500,

            bgColor: colors.transparent,
            borderColor: colors.transparent,
            hoverBgColor: colors.transparent,
            activeBgColor: colors.transparent,
            disabledBgColor: colors.transparent,
            disabledBorderColor: colors.transparent,
        };
    }

    // Filled variable is the base style
    return base;
};

// Generate color styles for button
const getButtonVariantStyles = (variant: ButtonVariant, colorStyles: ColorStyles): CSSObject => {
    const variantStyles = {
        filled: {
            backgroundColor: colorStyles.bgColor,
            border: `1px solid ${colorStyles.borderColor}`,
            color: colorStyles.textColor,
            '&:hover': {
                backgroundColor: colorStyles.hoverBgColor,
                border: `1px solid ${colorStyles.hoverBgColor}`,
                boxShadow: shadows.sm,
            },
            '&:disabled': {
                backgroundColor: colorStyles.disabledBgColor,
                border: `1px solid ${colorStyles.disabledBorderColor}`,
                color: colorStyles.disabledTextColor,
                boxShadow: shadows.xs,
            },
        },
        outline: {
            backgroundColor: 'transparent',
            border: `1px solid ${colorStyles.borderColor}`,
            color: colorStyles.textColor,
            '&:hover': {
                backgroundColor: colorStyles.hoverBgColor,
                boxShadow: 'none',
            },
            '&:disabled': {
                backgroundColor: colorStyles.disabledBgColor,
                border: `1px solid ${colorStyles.disabledBorderColor}`,
                color: colorStyles.disabledTextColor,
                boxShadow: shadows.xs,
            },
        },
        text: {
            backgroundColor: 'transparent',
            border: 'none',
            color: colorStyles.textColor,
            '&:hover': {
                backgroundColor: colorStyles.hoverBgColor,
            },
            '&:disabled': {
                backgroundColor: colorStyles.disabledBgColor,
                color: colorStyles.disabledTextColor,
            },
        },
    };

    return variantStyles[variant];
};

// Generate font styles for button
const getButtonFontStyles = (size: SizeOptions) => {
    const baseFontStyles = {
        fontFamily: typography.fonts.body,
        fontWeight: typography.fontWeights.normal,
        lineHeight: typography.lineHeights.none,
    };

    const sizeStyles = {
        sm: {
            ...baseFontStyles,
            fontSize: getFontSize(size), // 12px
        },
        md: {
            ...baseFontStyles,
            fontSize: getFontSize(size), // 14px
        },
        lg: {
            ...baseFontStyles,
            fontSize: getFontSize(size), // 16px
        },
        xl: {
            ...baseFontStyles,
            fontSize: getFontSize(size), // 18px
        },
    };

    return sizeStyles[size];
};

// Generate radii styles for button
const getButtonRadiiStyles = (isCircle: boolean) => {
    if (isCircle) return { borderRadius: radius.full };
    return { borderRadius: radius.sm }; // radius is the same for all button sizes
};

// Generate padding styles for button
const getButtonPadding = (size: SizeOptions, variant: ButtonVariant, isCircle: boolean) => {
    if (isCircle) return { padding: spacing.xsm };

    const paddingStyles = {
        xs: {
            vertical: 0,
            horizontal: 0,
        },
        sm: {
            vertical: 8,
            horizontal: 12,
        },
        md: {
            vertical: 10,
            horizontal: 12,
        },
        lg: {
            vertical: 10,
            horizontal: 16,
        },
        xl: {
            vertical: 12,
            horizontal: 20,
        },
    };

    const selectedStyle = paddingStyles[size];
    const verticalPadding = selectedStyle.vertical;
    const horizontalPadding = variant === 'text' ? 0 : selectedStyle.horizontal;
    return { padding: `${verticalPadding}px ${horizontalPadding}px` };
};

// Generate active styles for button
const getButtonActiveStyles = (colorStyles: ColorStyles) => ({
    borderColor: 'transparent',
    backgroundColor: colorStyles.activeBgColor,
    // TODO: Figure out how to make the #fff interior border transparent
    boxShadow: `0 0 0 2px #fff, 0 0 0 4px ${colorStyles.activeBgColor}`,
});

// Generate loading styles for button
const getButtonLoadingStyles = () => ({
    pointerEvents: 'none',
    opacity: 0.75,
});

/*
 * Main function to generate styles for button
 */
export const getButtonStyle = (props: ButtonStyleProps) => {
    const { variant, color, size, isCircle, isActive, isLoading, isDisabled } = props;

    // Get map of colors
    const colorStyles = getButtonColorStyles(variant, color);

    // Define styles for button
    const variantStyles = getButtonVariantStyles(variant, colorStyles);
    const fontStyles = getButtonFontStyles(size);
    const radiiStyles = getButtonRadiiStyles(isCircle);
    const paddingStyles = getButtonPadding(size, variant, isCircle);

    // Base of all generated styles
    let styles = {
        ...variantStyles,
        ...fontStyles,
        ...radiiStyles,
        ...paddingStyles,
    };

    // Focus & Active styles are the same, but active styles are applied conditionally & override prevs styles
    const activeStyles = { ...getButtonActiveStyles(colorStyles) };
    if (!isDisabled && isActive) {
        styles['&:focus'] = activeStyles;
        styles['&:active'] = activeStyles;
        styles = { ...styles, ...activeStyles };
    }

    // Loading styles
    if (isLoading) styles = { ...styles, ...getButtonLoadingStyles() };

    // Return generated styles
    return styles;
};

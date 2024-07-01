/*
 * Button Style Utilities
 */

import { text, colors, shadows, radii, spacing } from '../theme';
import { getColorValue, getFontSize } from '../theme/utils';

import { ButtonProps } from './types';

// Utility function to get color styles for button - does not generate CSS
const getButtonColorStyles = (variant, color) => {
    const color500 = getColorValue(color, 500); // value of 500 shade
    const isViolet = color === 'violet';

    const base = {
        // Backgrounds
        bgColor: color500,
        hoverBgColor: getColorValue(color, 600),
        activeBgColor: getColorValue(color, 700),
        disabledBgColor: getColorValue('gray', 100),

        // Borders
        borderColor: color500,
        activeBorderColor: getColorValue(color, 300),
        disabledBorderColor: getColorValue('gray', 200),

        // Text
        textColor: colors.white,
        disabledTextColor: getColorValue('gray', 300),
    };

    // Specific color override for white
    if (color === 'white') {
        base.textColor = colors.black;
        base.disabledTextColor = getColorValue('gray', 500);
    }

    // Specific color override for gray
    if (color === 'gray') {
        base.textColor = getColorValue('gray', 500);
        base.bgColor = getColorValue('gray', 50);
        base.borderColor = getColorValue('gray', 50);

        base.hoverBgColor = getColorValue('gray', 100);
        base.activeBgColor = getColorValue('gray', 200);
    }

    // Override styles for outline variant
    if (variant === 'outline') {
        return {
            ...base,
            bgColor: 'transparent',
            borderColor: color500,
            textColor: color500,

            hoverBgColor: isViolet ? getColorValue(color, 50) : getColorValue(color, 100),
            activeBgColor: isViolet ? getColorValue(color, 100) : getColorValue(color, 200),

            disabledBgColor: 'transparent',
        };
    }

    // Override styles for text variant
    if (variant === 'text') {
        return {
            ...base,
            bgColor: 'transparent',
            borderColor: 'transparent',
            textColor: color500,

            hoverBgColor: 'transparent',
            activeBgColor: 'transparent',

            disabledBgColor: 'transparent',
            disabledBorderColor: 'transparent',
        };
    }

    // Filled variable is the base style
    return base;
};

// Generate color styles for button
const getButtonVariantStyles = (variant, color) => {
    const variantStyles = {
        filled: {
            backgroundColor: color.bgColor,
            border: `1px solid ${color.borderColor}`,
            color: color.textColor,
            '&:hover': {
                backgroundColor: color.hoverBgColor,
                border: `1px solid ${color.hoverBgColor}`,
                boxShadow: shadows.sm,
            },
            '&:disabled': {
                backgroundColor: color.disabledBgColor,
                border: `1px solid ${color.disabledBorderColor}`,
                color: color.disabledTextColor,
                boxShadow: shadows.xs,
            },
        },
        outline: {
            backgroundColor: 'transparent',
            border: `1px solid ${color.borderColor}`,
            color: color.textColor,
            '&:hover': {
                backgroundColor: color.hoverBgColor,
                boxShadow: 'none',
            },
            '&:disabled': {
                backgroundColor: color.disabledBgColor,
                border: `1px solid ${color.disabledBorderColor}`,
                color: color.disabledTextColor,
                boxShadow: shadows.xs,
            },
        },
        text: {
            backgroundColor: 'transparent',
            border: 'none',
            color: color.textColor,
            '&:hover': {
                backgroundColor: color.hoverBgColor,
            },
            '&:disabled': {
                backgroundColor: color.disabledBgColor,
                color: color.disabledTextColor,
            },
        },
    };

    return variantStyles[variant];
};

// Generate font styles for button
const getButtonFontStyles = (size) => {
    const baseFontStyles = {
        fontFamily: text.family.default,
        fontWeight: text.weight.light,
        lineHeight: text.lineHeight.normal,
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
const getButtonRadiiStyles = (isCircle) => {
    if (isCircle) return { borderRadius: '100%' };
    return { borderRadius: radii.sm }; // radius is the same for all button sizes
};

// Generate padding styles for button
const getButtonPadding = (size, isCircle) => {
    if (isCircle) return { padding: spacing.xsm };

    const paddingStyles = {
        sm: {
            padding: '8px 12px',
        },
        md: {
            padding: '10px 12px',
        },
        lg: {
            padding: '10px 16px',
        },
        xl: {
            padding: '12px 20px',
        },
    };

    return paddingStyles[size];
};

// Generate active styles for button
const getButtonActiveStyles = (styleColors) => ({
    borderColor: 'transparent',
    backgroundColor: styleColors.activeBgColor,
    // TODO: Figure out how to make the #fff interior border transparent
    boxShadow: `0 0 0 2px #fff, 0 0 0 4px ${styleColors.activeBgColor}`,
});

// Generate loading styles for button
const getButtonLoadingStyles = () => ({
    pointerEvents: 'none',
    opacity: 0.75,
});

/*
 * Main function to generate styles for button
 */
export const getButtonStyle = (props: ButtonProps) => {
    const { variant, color, size, isCircle, isActive, isLoading } = props;

    // Get map of colors
    const colorStyles = getButtonColorStyles(variant, color) || ({} as any);

    // Define styles for button
    const variantStyles = getButtonVariantStyles(variant, colorStyles);
    const fontStyles = getButtonFontStyles(size);
    const radiiStyles = getButtonRadiiStyles(isCircle);
    const paddingStyles = getButtonPadding(size, isCircle);

    // Base of all generated styles
    let styles = {
        ...variantStyles,
        ...fontStyles,
        ...radiiStyles,
        ...paddingStyles,
    };

    // Focus & Active styles are the same, but active styles are applied conditionally & override prevs styles
    const activeStyles = { ...getButtonActiveStyles(colorStyles) };
    styles['&:focus'] = activeStyles;
    styles['&:active'] = activeStyles;
    if (isActive) styles = { ...styles, ...activeStyles };

    // Loading styles
    if (isLoading) styles = { ...styles, ...getButtonLoadingStyles() };

    // Return generated styles
    return styles;
};

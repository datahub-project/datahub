/*
 * Button Style Utilities
 */

import { typography, colors, shadows, radius, spacing } from '@components/theme';
import { getColor, getFontSize } from '@components/theme/utils';
import { ButtonProps } from './types';

// Utility function to get color styles for button - does not generate CSS
const getButtonColorStyles = (variant, color) => {
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
const getButtonRadiiStyles = (isCircle) => {
    if (isCircle) return { borderRadius: radius.full };
    return { borderRadius: radius.sm }; // radius is the same for all button sizes
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

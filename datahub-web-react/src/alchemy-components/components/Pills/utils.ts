import { colors, typography } from '@src/alchemy-components/theme';
import { getColor, getFontSize } from '@src/alchemy-components/theme/utils';
import { PillStyleProps } from './types';

// Utility function to get color styles for pill - does not generate CSS
const getPillColorStyles = (variant, color) => {
    const defaultStyles = {
        bgColor: getColor(color, 100),
        hoverBgColor: getColor('gray', 100),
        borderColor: '',
        activeBorderColor: getColor('violet', 500),
        textColor: getColor(color, 600),
    };

    const colorOverrides = {
        violet: {
            textColor: getColor(color, 500),
            bgColor: getColor('gray', 1000),
            borderColor: 'transparent',
            hoverBgColor: getColor(color, 100),
            activeBorderColor: getColor(color, 500),
        },
        blue: {
            textColor: getColor(color, 1000),
            bgColor: getColor('gray', 1100),
            borderColor: 'transparent',
            hoverBgColor: getColor(color, 1100),
            activeBorderColor: getColor(color, 1000),
        },
        red: {
            textColor: getColor(color, 1000),
            bgColor: getColor('gray', 1200),
            hoverBgColor: getColor(color, 1100),
            activeBorderColor: getColor(color, 1000),
        },
        green: {
            textColor: getColor(color, 1000),
            bgColor: getColor('gray', 1300),
            hoverBgColor: getColor(color, 1100),
            activeBorderColor: getColor(color, 1000),
        },
        yellow: {
            textColor: getColor(color, 1000),
            bgColor: getColor('gray', 1400),
            hoverBgColor: getColor(color, 1100),
            activeBorderColor: getColor(color, 1000),
        },
    };

    const styles = colorOverrides[color] || defaultStyles;

    if (variant === 'outline') {
        return {
            bgColor: colors.transparent,
            borderColor: getColor('gray', 1400),
            textColor: getColor(color, 600),
        };
    }

    return styles;
};

// Generate variant styles for pill
const getPillVariantStyles = (variant, colorStyles) =>
    ({
        filled: {
            backgroundColor: colorStyles.bgColor,
            border: `1px solid transparent`,
            color: colorStyles.textColor,
            '&:hover': {
                backgroundColor: colorStyles.hoverBgColor,
            },
        },
        outline: {
            backgroundColor: 'transparent',
            border: `1px solid ${colorStyles.borderColor}`,
            color: colorStyles.textColor,
            '&:hover': {
                backgroundColor: colorStyles.hoverBgColor,
                border: `1px solid transparent`,
            },
            '&:disabled': {
                border: `1px solid transparent`,
            },
        },
        text: {
            color: colorStyles.textColor,
        },
    }[variant]);

// Generate font styles for pill
const getPillFontStyles = (size) => {
    const baseFontStyles = {
        fontFamily: typography.fonts.body,
        fontWeight: typography.fontWeights.normal,
        lineHeight: typography.lineHeights.none,
    };

    const sizeMap = {
        xs: { fontSize: getFontSize(size), lineHeight: '16px' },
        sm: { fontSize: getFontSize(size), lineHeight: '22px' },
        md: { fontSize: getFontSize(size), lineHeight: '24px' },
        lg: { fontSize: getFontSize(size), lineHeight: '30px' },
        xl: { fontSize: getFontSize(size), lineHeight: '34px' },
    };

    return {
        ...baseFontStyles,
        ...sizeMap[size],
    };
};

// Generate active styles for pill
const getPillActiveStyles = (styleColors) => ({
    borderColor: styleColors.activeBorderColor,
});

/*
 * Main function to generate styles for pill
 */
export const getPillStyle = (props: PillStyleProps) => {
    const { variant, colorScheme = 'gray', size, clickable = true } = props;

    // Get map of colors
    const colorStyles = getPillColorStyles(variant, colorScheme);

    // Define styles for pill
    let styles = {
        ...getPillVariantStyles(variant, colorStyles),
        ...getPillFontStyles(size),
        '&:focus': {
            ...getPillActiveStyles(colorStyles),
            outline: 'none', // Remove default browser focus outline if needed
        },
        '&:active': {
            ...getPillActiveStyles(colorStyles),
        },
    };
    if (!clickable) {
        styles = {
            ...styles,
            pointerEvents: 'none',
        };
    }

    return styles;
};

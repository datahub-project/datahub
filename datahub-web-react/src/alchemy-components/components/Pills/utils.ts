import { PillStyleProps } from '@components/components/Pills/types';
import { ColorOptions, PillVariantOptions, SizeOptions } from '@components/theme/config';
import { typography } from '@src/alchemy-components/theme';
import { getColor, getFontSize } from '@src/alchemy-components/theme/utils';
import { CSSObject } from 'styled-components';

interface ColorStyles {
    primaryColor: string;
    bgColor: string;
    borderColor: string;
    hoverColor?: string;
}

// Utility function to get color styles for pill - does not generate CSS
function getPillColorStyles(variant: PillVariantOptions, color: ColorOptions): ColorStyles {
    if (variant === 'version') {
        return {
            bgColor: getColor('gray', color === 'white' ? 1500 : 100),
            borderColor: getColor('gray', 100),
            primaryColor: getColor('gray', 1700),
        };
    }

    return {
        primaryColor: getColor(color, 500),
        bgColor: color === 'gray' ? getColor(color, 100) : getColor(color, 0),
        hoverColor: color === 'gray' ? getColor(color, 100) : getColor(color, 1100),
        borderColor: getColor('gray', 1400),
    };
}

// Generate variant styles for pill
const getPillVariantStyles = (variant: PillVariantOptions, colorStyles: ColorStyles): CSSObject =>
    ({
        filled: {
            backgroundColor: colorStyles.bgColor,
            border: `1px solid transparent`,
            color: colorStyles.primaryColor,
            '&:hover': {
                backgroundColor: colorStyles.hoverColor,
            },
        },
        outline: {
            backgroundColor: 'transparent',
            border: `1px solid ${colorStyles.primaryColor}`,
            color: colorStyles.primaryColor,
            '&:hover': {
                backgroundColor: colorStyles.hoverColor,
                border: `1px solid transparent`,
            },
            '&:disabled': {
                border: `1px solid transparent`,
            },
        },
        text: {
            color: colorStyles.primaryColor,
        },
        version: {
            backgroundColor: colorStyles.bgColor,
            border: `1px solid ${colorStyles.borderColor}`,
            color: colorStyles.primaryColor,
            '&:hover': {
                backgroundColor: colorStyles.hoverColor,
            },
            borderRadius: '4px',
        },
    }[variant]);

const getPillFontStyles = (variant: PillVariantOptions, size: SizeOptions): CSSObject => {
    const baseFontStyles = {
        fontFamily: typography.fonts.body,
        fontWeight: typography.fontWeights.normal,
        lineHeight: typography.lineHeights.none,
    };

    const sizeMap: Record<SizeOptions, CSSObject> = {
        xs: { fontSize: getFontSize(size), lineHeight: '16px' },
        sm: { fontSize: getFontSize(size), lineHeight: '22px' },
        md: { fontSize: getFontSize(size), lineHeight: '24px' },
        lg: { fontSize: getFontSize(size), lineHeight: '30px' },
        xl: { fontSize: getFontSize(size), lineHeight: '34px' },
    };

    const variantOverrides: Partial<Record<PillVariantOptions, CSSObject>> = {
        version: {
            fontWeight: typography.fontWeights.semiBold,
            lineHeight: 1.4,
        },
    };

    return {
        ...baseFontStyles,
        ...sizeMap[size],
        ...variantOverrides[variant],
    };
};

const getPillActiveStyles = (variant: PillVariantOptions, colorStyles: ColorStyles): CSSObject => ({
    borderColor: variant === 'filled' || variant === 'outline' ? colorStyles.primaryColor : '',
});

export function getPillStyle(props: PillStyleProps): CSSObject {
    const { variant, color, size, clickable = true } = props;

    // Get map of colors
    const colorStyles = getPillColorStyles(variant, color);

    // Define styles for pill
    let styles = {
        ...getPillVariantStyles(variant, colorStyles),
        ...getPillFontStyles(variant, size),
        '&:focus': {
            ...getPillActiveStyles(variant, colorStyles),
            outline: 'none', // Remove default browser focus outline if needed
        },
        '&:active': {
            ...getPillActiveStyles(variant, colorStyles),
        },
    };
    if (!clickable) {
        styles = {
            ...styles,
            pointerEvents: 'none',
        };
    }

    return styles;
}

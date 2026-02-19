import { CSSObject } from 'styled-components';

import { PillStyleProps } from '@components/components/Pills/types';
import { ColorOptions, PillVariantOptions, SizeOptions } from '@components/theme/config';

import { Theme } from '@conf/theme/types';
import { typography } from '@src/alchemy-components/theme';
import { getColor, getFontSize } from '@src/alchemy-components/theme/utils';

interface ColorStyles {
    primaryColor: string;
    bgColor: string;
    borderColor: string;
    hoverColor?: string;
}

function getSemanticPillColors(color: ColorOptions, themeColors: Record<string, string>): ColorStyles | null {
    const map: Partial<Record<ColorOptions, ColorStyles>> = {
        green: {
            primaryColor: themeColors.textSuccess,
            bgColor: themeColors.bgSurfaceSuccess,
            hoverColor: themeColors.bgSurfaceSuccessHover,
            borderColor: themeColors.borderSuccess,
        },
        red: {
            primaryColor: themeColors.textError,
            bgColor: themeColors.bgSurfaceError,
            hoverColor: themeColors.bgSurfaceErrorHover,
            borderColor: themeColors.borderError,
        },
        blue: {
            primaryColor: themeColors.textInformation,
            bgColor: themeColors.bgSurfaceInfo,
            hoverColor: themeColors.bgSurfaceInformationHover,
            borderColor: themeColors.borderInformation,
        },
        yellow: {
            primaryColor: themeColors.textWarning,
            bgColor: themeColors.bgSurfaceWarning,
            hoverColor: themeColors.bgSurfaceWarningHover,
            borderColor: themeColors.borderWarning,
        },
        violet: {
            primaryColor: themeColors.textBrand,
            bgColor: themeColors.bgSurfaceBrand,
            hoverColor: themeColors.bgSurfaceBrandHover,
            borderColor: themeColors.borderBrand,
        },
        primary: {
            primaryColor: themeColors.textBrand,
            bgColor: themeColors.bgSurfaceBrand,
            hoverColor: themeColors.bgSurfaceBrandHover,
            borderColor: themeColors.borderBrand,
        },
        gray: {
            primaryColor: themeColors.textSecondary,
            bgColor: themeColors.bgSurface,
            hoverColor: themeColors.bgHover,
            borderColor: themeColors.border,
        },
        white: {
            primaryColor: themeColors.textSecondary,
            bgColor: themeColors.bgSurface,
            hoverColor: themeColors.bgHover,
            borderColor: themeColors.border,
        },
    };
    return map[color] ?? null;
}

// Utility function to get color styles for pill - does not generate CSS
function getPillColorStyles(variant: PillVariantOptions, color: ColorOptions, theme?: Theme): ColorStyles {
    const themeColors = theme?.colors as Record<string, string> | undefined;

    if (variant === 'version' && themeColors) {
        return {
            bgColor: themeColors.bgSurface,
            borderColor: themeColors.border,
            primaryColor: themeColors.textSecondary,
        };
    }

    if (themeColors) {
        const semantic = getSemanticPillColors(color, themeColors);
        if (semantic) return semantic;
    }

    return {
        primaryColor: getColor(color, 700, theme),
        bgColor: color === 'gray' ? getColor(color, 100, theme) : getColor(color, 0, theme),
        hoverColor: color === 'gray' ? getColor(color, 100, theme) : getColor(color, 1100, theme),
        borderColor: getColor('gray', 1800, theme),
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
            border: `1px solid ${colorStyles.bgColor}`,
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
    })[variant];

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
        inherit: { fontSize: 'inherit', lineHeight: 'inherit' },
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
    const { variant, color, size, clickable = true, theme } = props;

    // Get map of colors
    const colorStyles = getPillColorStyles(variant, color, theme);

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

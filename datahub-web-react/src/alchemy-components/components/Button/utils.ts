/*
 * Button Style Utilities
 */
import { ButtonHTMLAttributes } from 'react';
import { CSSObject } from 'styled-components';

import { ButtonStyleProps, ButtonVariant } from '@components/components/Button/types';
import { radius, shadows, spacing, typography } from '@components/theme';
import { ColorOptions, SizeOptions } from '@components/theme/config';
import { getColor, getFontSize } from '@components/theme/utils';

import { Theme } from '@conf/theme/types';

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

/**
 * Foreground color for text / outline / link / secondary buttons.
 * Uses semantic theme tokens (same ones Alert titles use) so status colors
 * stay readable on tinted surfaces — foundation shade 500 is often too light
 * (e.g. green[500] #77B750 vs textSuccess #0D7543).
 */
const getSemanticForegroundColor = (color: ColorOptions, theme: Theme): string | undefined => {
    const themeColors = theme?.colors;
    if (!themeColors) return undefined;

    switch (color) {
        case 'green':
            return themeColors.textSuccess;
        case 'red':
            return themeColors.textError;
        case 'blue':
            return themeColors.textInformation;
        case 'yellow':
            return themeColors.textWarning;
        case 'primary':
        case 'violet':
            return themeColors.textBrand ?? themeColors.buttonFillBrand;
        case 'gray':
            return themeColors.textSecondary;
        default:
            return undefined;
    }
};

/**
 * Soft surface backgrounds for secondary buttons, matched to the button color
 * (same semantic surfaces Alert banners use). Brand/primary keep the violet
 * brand surfaces; other colors previously incorrectly inherited those too.
 */
const getSecondarySurfaceColors = (
    color: ColorOptions,
    theme: Theme,
): Pick<ColorStyles, 'bgColor' | 'hoverBgColor' | 'activeBgColor'> => {
    const themeColors = theme?.colors;

    switch (color) {
        case 'green':
            return {
                bgColor: themeColors?.bgSurfaceSuccess ?? getColor('green', 0, theme),
                hoverBgColor: themeColors?.bgSurfaceSuccessHover ?? getColor('green', 100, theme),
                activeBgColor: themeColors?.bgSurfaceSuccessHover ?? getColor('green', 100, theme),
            };
        case 'red':
            return {
                bgColor: themeColors?.bgSurfaceError ?? getColor('red', 0, theme),
                hoverBgColor: themeColors?.bgSurfaceErrorHover ?? getColor('red', 100, theme),
                activeBgColor: themeColors?.bgSurfaceErrorHover ?? getColor('red', 100, theme),
            };
        case 'blue':
            return {
                bgColor: themeColors?.bgSurfaceInfo ?? getColor('blue', 0, theme),
                hoverBgColor: themeColors?.bgSurfaceInformationHover ?? getColor('blue', 100, theme),
                activeBgColor: themeColors?.bgSurfaceInformationHover ?? getColor('blue', 100, theme),
            };
        case 'yellow':
            return {
                bgColor: themeColors?.bgSurfaceWarning ?? getColor('yellow', 0, theme),
                hoverBgColor: themeColors?.bgSurfaceWarningHover ?? getColor('yellow', 100, theme),
                activeBgColor: themeColors?.bgSurfaceWarningHover ?? getColor('yellow', 100, theme),
            };
        case 'gray':
            return {
                bgColor: themeColors?.bgSurface ?? getColor('gray', 100, theme),
                hoverBgColor: themeColors?.bgHover ?? getColor('gray', 100, theme),
                activeBgColor: themeColors?.bgActive ?? getColor('gray', 200, theme),
            };
        case 'primary':
        case 'violet':
        default:
            return {
                bgColor: themeColors?.bgSurfaceBrand ?? getColor('violet', 0, theme),
                hoverBgColor: themeColors?.bgSurfaceBrandHover ?? getColor('violet', 100, theme),
                activeBgColor: themeColors?.buttonSurfaceBrandFocus ?? getColor('violet', 200, theme),
            };
    }
};

// Utility function to get color styles for button - does not generate CSS
const getButtonColorStyles = (variant: ButtonVariant, color: ColorOptions, theme: Theme): ColorStyles => {
    const isViolet = color === 'violet';
    const isPrimary = isViolet || color === 'primary';
    // Brand (primary/violet) filled buttons must follow the configurable CI brand color rather
    // than the static foundation ramp. buttonFillBrand pairs with brandGradient.
    const color500 =
        isPrimary && theme?.colors?.buttonFillBrand ? theme.colors.buttonFillBrand : getColor(color, 500, theme); // value of 500 shade
    // Readable on-surface color for non-filled variants (matches Alert / banner text).
    const foregroundColor = getSemanticForegroundColor(color, theme) ?? color500;

    const base = {
        // Backgrounds
        bgColor: color500,
        hoverBgColor: color500,
        activeBgColor: getColor(color, 700, theme),
        disabledBgColor: theme?.colors?.bgDisabled ?? getColor('gray', 100, theme),

        // Borders
        borderColor: color500,
        activeBorderColor: getColor(color, 300, theme),
        disabledBorderColor: theme?.colors?.borderDisabled ?? getColor('gray', 200, theme),

        // Text
        textColor: theme?.colors?.textBrandOnBgFill ?? 'white',
        disabledTextColor: theme?.colors?.textDisabled ?? getColor('gray', 300, theme),
    };

    if (color === 'white') {
        base.textColor = theme?.colors?.text ?? 'black';
        base.disabledTextColor = theme?.colors?.textDisabled ?? getColor('gray', 500, theme);
    }

    if (color === 'gray') {
        base.textColor = theme?.colors?.textSecondary ?? getColor('gray', 500, theme);
        base.bgColor = theme?.colors?.bgSurface ?? getColor('gray', 100, theme);
        base.borderColor = theme?.colors?.bgSurface ?? getColor('gray', 100, theme);
        base.hoverBgColor = theme?.colors?.bgHover ?? getColor('gray', 100, theme);
        base.activeBgColor = theme?.colors?.bgActive ?? getColor('gray', 200, theme);
    }

    // Override styles for outline variant
    if (variant === 'outline') {
        return {
            ...base,
            bgColor: 'transparent',
            borderColor: foregroundColor,
            textColor: foregroundColor,

            hoverBgColor: getColor(color, 100, theme),
            activeBgColor: isViolet ? getColor(color, 100, theme) : getColor(color, 200, theme),

            disabledBgColor: 'transparent',
        };
    }

    // Override styles for text variant
    if (variant === 'text') {
        return {
            ...base,
            textColor: foregroundColor,

            bgColor: 'transparent',
            borderColor: 'transparent',
            hoverBgColor: theme?.colors?.bgHover ?? 'transparent',
            activeBgColor: 'transparent',
            disabledBgColor: 'transparent',
            disabledBorderColor: 'transparent',
        };
    }

    // Override styles for secondary variant
    if (variant === 'secondary') {
        const secondarySurfaces = getSecondarySurfaceColors(color, theme);
        return {
            ...base,
            bgColor: secondarySurfaces.bgColor,
            hoverBgColor: secondarySurfaces.hoverBgColor,
            activeBgColor: secondarySurfaces.activeBgColor,
            textColor: foregroundColor,
            borderColor: 'transparent',
            disabledBgColor: 'transparent',
            disabledBorderColor: 'transparent',
        };
    }

    // Override styles for link variant
    if (variant === 'link') {
        return {
            ...base,
            textColor: foregroundColor,
            bgColor: 'transparent',
            borderColor: 'transparent',
            activeBgColor: 'transparent',
            disabledBgColor: 'transparent',
            disabledBorderColor: 'transparent',
        };
    }

    // Filled variable is the base style
    return base;
};

// Generate color styles for button
const getButtonVariantStyles = (
    variant: ButtonVariant,
    colorStyles: ColorStyles,
    color: ColorOptions,
    theme?: Theme,
): CSSObject => {
    const isPrimary = color === 'violet' || color === 'primary';

    const primaryGradient = theme?.colors?.brandGradient ?? colorStyles.bgColor;

    const variantStyles = {
        filled: {
            background: isPrimary ? primaryGradient : colorStyles.bgColor,
            border: `1px solid ${colorStyles.borderColor}`,
            color: colorStyles.textColor,
            '&:hover': {
                background: isPrimary ? primaryGradient : colorStyles.hoverBgColor,
                border: `1px solid ${colorStyles.hoverBgColor}`,
                boxShadow: shadows.sm,
            },
            '&:disabled': {
                backgroundColor: colorStyles.disabledBgColor,
                background: 'none',
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
        secondary: {
            backgroundColor: colorStyles.bgColor,
            border: 'none',
            color: colorStyles.textColor,
            '&:hover': {
                backgroundColor: colorStyles.hoverBgColor,
                boxShadow: 'none',
            },
            '&:disabled': {
                backgroundColor: colorStyles.disabledBgColor,
                color: colorStyles.disabledTextColor,
            },
        },
        link: {
            backgroundColor: 'transparent',
            border: 'none',
            color: colorStyles.textColor,
            padding: 0,
            '&:hover': {
                textDecoration: 'underline',
            },
            '&:disabled': {
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
        fontWeight: typography.fontWeights.semiBold,
        lineHeight: typography.lineHeights.none,
    };

    return { ...baseFontStyles, fontSize: getFontSize(size) };
};

// Generate radii styles for button
const getButtonRadiiStyles = (isCircle: boolean) => {
    if (isCircle) return { borderRadius: radius.full };
    return { borderRadius: radius.sm }; // radius is the same for all button sizes
};

// Generate padding styles for button
const getButtonPadding = (size: SizeOptions, hasChildren: boolean, isCircle: boolean, variant: ButtonVariant) => {
    if (isCircle) return { padding: spacing.xsm };
    if (!hasChildren) return { padding: spacing.xsm };
    if (variant === 'link') return { padding: 0 };

    const paddingStyles = {
        xs: {
            vertical: 6,
            horizontal: 8,
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
    const horizontalPadding = selectedStyle.horizontal;
    return { padding: `${verticalPadding}px ${horizontalPadding}px` };
};

// Generate active styles for button
const getButtonActiveStyles = (colorStyles: ColorStyles, theme?: Theme) => ({
    borderColor: 'transparent',
    backgroundColor: colorStyles.activeBgColor,
    boxShadow: `0 0 0 2px ${theme?.colors?.bg ?? 'transparent'}, 0 0 0 4px ${colorStyles.activeBgColor}`,
});

// Generate loading styles for button
const getButtonLoadingStyles = (): CSSObject => ({
    pointerEvents: 'none',
    opacity: 0.75,
});

/*
 * Main function to generate styles for button
 */
export const getButtonStyle = (props: ButtonStyleProps & ButtonHTMLAttributes<HTMLButtonElement>): CSSObject => {
    const { variant, color, size, isCircle, isActive, isLoading, disabled, hasChildren, theme } = props;

    // Get map of colors
    const colorStyles = getButtonColorStyles(variant, color, theme);

    // Define styles for button
    const variantStyles = getButtonVariantStyles(variant, colorStyles, color, theme);
    const fontStyles = getButtonFontStyles(size);
    const radiiStyles = getButtonRadiiStyles(isCircle);
    const paddingStyles = getButtonPadding(size, hasChildren, isCircle, variant);

    // Base of all generated styles
    let styles: CSSObject = {
        ...variantStyles,
        ...fontStyles,
        ...radiiStyles,
        ...paddingStyles,
    };

    // Focus & Active styles are the same, but active styles are applied conditionally & override prevs styles
    const activeStyles = { ...getButtonActiveStyles(colorStyles, theme) };
    if (!disabled && isActive) {
        styles['&:focus'] = activeStyles;
        styles['&:active'] = activeStyles;
        styles = { ...styles, ...activeStyles };
    }

    // Loading styles
    if (isLoading) styles = { ...styles, ...getButtonLoadingStyles() };

    // Return generated styles
    return styles;
};

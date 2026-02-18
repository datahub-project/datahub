import { colors } from '@components/theme';

import { SizeOptions } from '@src/alchemy-components/theme/config';

const checkboxBackgroundDefault = {
    default: colors.white,
    checked: colors.violet[500],
    error: colors.red[500],
    disabled: colors.gray[1500],
};

const checkboxHoverColors = {
    default: colors.gray[100],
    error: colors.red[100],
    checked: colors.violet[100],
};

export function getCheckboxColor(
    checked: boolean,
    error: string,
    disabled: boolean,
    mode: 'background' | undefined,
    themeColors?: {
        bg: string;
        bgSurface: string;
        bgDisabled: string;
        border: string;
        textTertiary: string;
        textError: string;
        iconBrand: string;
    },
) {
    if (disabled) {
        return mode === 'background'
            ? (themeColors?.bgDisabled ?? checkboxBackgroundDefault.disabled)
            : (themeColors?.border ?? colors.gray[100]);
    }
    if (error) return themeColors?.textError ?? checkboxBackgroundDefault.error;
    if (checked) return themeColors?.iconBrand ?? checkboxBackgroundDefault.checked;
    return mode === 'background'
        ? (themeColors?.bg ?? checkboxBackgroundDefault.default)
        : (themeColors?.textTertiary ?? colors.gray[1800]);
}

export function getCheckboxHoverBackgroundColor(
    checked: boolean,
    error: string,
    themeColors?: { bgHover: string; bgSurfaceErrorHover: string; bgSurfaceBrandHover: string },
) {
    if (error) return themeColors?.bgSurfaceErrorHover ?? checkboxHoverColors.error;
    if (checked) return themeColors?.bgSurfaceBrandHover ?? checkboxHoverColors.checked;
    return themeColors?.bgHover ?? checkboxHoverColors.default;
}

const sizeMap: Record<SizeOptions, string> = {
    xs: '16px',
    sm: '18px',
    md: '20px',
    lg: '22px',
    xl: '24px',
    inherit: 'inherit',
};

export function getCheckboxSize(size: SizeOptions) {
    return {
        height: sizeMap[size],
        width: sizeMap[size],
    };
}

const offsetMap: Record<SizeOptions, string> = {
    xs: '7px',
    sm: '6px',
    md: '5px',
    lg: '4px',
    xl: '3px',
    inherit: 'inherit',
};

export function getCheckmarkPosition(size: SizeOptions) {
    return {
        top: offsetMap[size],
        left: offsetMap[size],
    };
}

import { SizeOptions } from '@src/alchemy-components/theme/config';

export function getCheckboxColor(
    checked: boolean,
    error: string,
    disabled: boolean,
    mode: 'background' | undefined,
    themeColors: {
        bg: string;
        bgSurface: string;
        bgDisabled: string;
        border: string;
        textError: string;
        iconBrand: string;
    },
) {
    if (disabled) {
        return mode === 'background' ? themeColors.bgDisabled : themeColors.border;
    }
    if (error) return themeColors.textError;
    if (checked) return themeColors.iconBrand;
    return mode === 'background' ? themeColors.bg : themeColors.border;
}

export function getCheckboxHoverBackgroundColor(
    checked: boolean,
    error: string,
    themeColors: { bgHover: string; bgSurfaceErrorHover: string; bgSurfaceBrandHover: string },
) {
    if (error) return themeColors.bgSurfaceErrorHover;
    if (checked) return themeColors.bgSurfaceBrandHover;
    return themeColors.bgHover;
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

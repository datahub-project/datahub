import ColorTheme from '@conf/theme/colorThemes/types';
import { SizeOptions } from '@src/alchemy-components/theme/config';

export function getCheckboxBorder(checked: boolean, error: boolean, disabled: boolean, colors?: ColorTheme) {
    if (checked) return 'none';
    if (error) return `1px solid ${colors?.borderError}`;
    if (disabled) return `1px solid ${colors?.borderDisabled}`;
    return `1px solid ${colors?.borderCheckbox}`;
}

const sizeMap: Record<SizeOptions, string> = {
    xs: '14px',
    sm: '16px',
    md: '18px',
    lg: '20px',
    xl: '22px',
    inherit: 'inherit',
};

export function getCheckboxSize(size: SizeOptions) {
    return sizeMap[size];
}

const touchTargetMap: Record<SizeOptions, string> = {
    xs: '18px',
    sm: '20px',
    md: '22px',
    lg: '24px',
    xl: '24px',
    inherit: '22px',
};

export function getCheckboxTouchTarget(size: SizeOptions) {
    return touchTargetMap[size];
}

const iconSizeMap: Record<SizeOptions, number> = {
    xs: 10,
    sm: 12,
    md: 14,
    lg: 16,
    xl: 18,
    inherit: 14,
};

export function getCheckIconSize(size: SizeOptions) {
    return iconSizeMap[size];
}

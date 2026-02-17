import { colors } from '@components/theme';

const radioBorderColors = {
    default: colors.gray[400],
    disabled: colors.gray[300],
    error: colors.red[500],
};

const radioCheckmarkColors = {
    default: colors.white,
    disabled: colors.gray[300],
    checked: colors.violet[500],
    error: colors.red[500],
};

export function getRadioBorderColor(
    disabled: boolean,
    error: string,
    themeColors?: { radioButtonBorder: string; borderDisabled: string; textError: string },
) {
    if (disabled) return themeColors?.borderDisabled ?? radioBorderColors.disabled;
    if (error) return themeColors?.textError ?? radioCheckmarkColors.error;
    return themeColors?.radioButtonBorder ?? radioBorderColors.default;
}

export function getRadioCheckmarkColor(
    checked: boolean,
    disabled: boolean,
    error: string,
    themeColors?: {
        radioButtonDotFill: string;
        radioButtonDotDisabled: string;
        iconBrand: string;
        textError: string;
    },
) {
    if (disabled) return themeColors?.radioButtonDotDisabled ?? radioCheckmarkColors.disabled;
    if (error) return themeColors?.textError ?? radioCheckmarkColors.error;
    if (checked) return themeColors?.iconBrand ?? radioCheckmarkColors.checked;
    return themeColors?.radioButtonDotFill ?? radioCheckmarkColors.default;
}

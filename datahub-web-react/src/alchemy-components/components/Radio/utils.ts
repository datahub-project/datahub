export function getRadioBorderColor(
    disabled: boolean,
    error: string,
    themeColors: { radioButtonBorder: string; borderDisabled: string; textError: string },
) {
    if (disabled) return themeColors.borderDisabled;
    if (error) return themeColors.textError;
    return themeColors.radioButtonBorder;
}

export function getRadioCheckmarkColor(
    checked: boolean,
    disabled: boolean,
    error: string,
    themeColors: {
        radioButtonDotFill: string;
        radioButtonDotDisabled: string;
        iconBrand: string;
        textError: string;
    },
) {
    if (disabled) return themeColors.radioButtonDotDisabled;
    if (error) return themeColors.textError;
    if (checked) return themeColors.iconBrand;
    return themeColors.radioButtonDotFill;
}

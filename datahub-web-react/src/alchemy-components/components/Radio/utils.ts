/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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

export function getRadioBorderColor(disabled: boolean, error: string) {
    if (disabled) return radioBorderColors.disabled;
    if (error) return radioCheckmarkColors.error;
    return radioBorderColors.default;
}

export function getRadioCheckmarkColor(checked: boolean, disabled: boolean, error: string) {
    if (disabled) return radioCheckmarkColors.disabled;
    if (error) return radioCheckmarkColors.error;
    if (checked) return radioCheckmarkColors.checked;
    return radioCheckmarkColors.default;
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

export const decimalToPercentStr = (decimal?: number | null, precision = 2): string => {
    if (!decimal) return '0%';
    return `${(decimal * 100).toFixed(precision)}%`;
};

export const percentStrToDecimal = (percentStr: string): number => {
    const value = parseFloat(percentStr.replace('%', '').trim());
    return Number.isNaN(value) ? 0 : value / 100;
};


export const decimalToPercentStr = (decimal?: number | null, precision: number = 2): string => {
    if (!decimal) return '0%';
    return `${(decimal * 100).toFixed(precision)}%`;
};

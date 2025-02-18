export const decimalToPercentStr = (decimal?: number | null, precision = 2): string => {
    if (!decimal) return '0%';
    return `${(decimal * 100).toFixed(precision)}%`;
};

export const percentStrToDecimal = (percentStr: string): number => {
    const value = parseFloat(percentStr.replace('%', '').trim());
    return Number.isNaN(value) ? 0 : value / 100;
};

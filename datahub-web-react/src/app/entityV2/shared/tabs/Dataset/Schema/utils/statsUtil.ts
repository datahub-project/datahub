import { UsageQueryResult } from '@types';

/** How often a field was used, from the dataset's usage aggregations; 0 when it has none. */
export const getFieldUsageCount = (usageStats: UsageQueryResult | null | undefined, fieldPath: string): number =>
    usageStats?.aggregations?.fields?.find((field) => field?.fieldName === fieldPath)?.count ?? 0;

export const decimalToPercentStr = (decimal?: number | null, precision = 2): string => {
    if (!decimal) return '0%';
    return `${(decimal * 100).toFixed(precision)}%`;
};

export const percentStrToDecimal = (percentStr: string): number => {
    const value = parseFloat(percentStr.replace('%', '').trim());
    return Number.isNaN(value) ? 0 : value / 100;
};

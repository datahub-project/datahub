/**
 * Normalizes a percentile integer to a 3-tier
 * label system of 'High', 'Med', 'Low' for usability.
 */
export const percentileToLabel = (pct: number) => {
    /* eslint-disable no-else-return */
    if (pct <= 30) {
        return 'Low';
    } else if (pct > 30 && pct <= 80) {
        return 'Med';
    }
    return 'High';
};

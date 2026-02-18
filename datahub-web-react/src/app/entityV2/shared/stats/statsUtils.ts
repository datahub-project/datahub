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

/**
 * Normalizes a percentile to a color.
 */
export const percentileToColor = (pct: number, themeColors?: { bgSurface?: string; bgSurfaceSuccess?: string }) => {
    /* eslint-disable no-else-return */
    if (pct <= 30) {
        return themeColors?.bgSurface ?? '#F5F5F5';
    } else if (pct > 30 && pct <= 80) {
        return themeColors?.bgSurfaceSuccess ?? '#EBF3F2';
    }
    return themeColors?.bgSurfaceSuccess ?? '#cef5f0';
};

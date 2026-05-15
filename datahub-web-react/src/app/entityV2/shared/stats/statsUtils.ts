import { DefaultTheme } from 'styled-components';

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
export const percentileToColor = (pct: number, theme: DefaultTheme) => {
    /* eslint-disable no-else-return */
    if (pct <= 30) {
        return theme.colors.bgSurface;
    } else if (pct > 30 && pct <= 80) {
        return theme.colors.bgSurfaceSuccess;
    }
    return theme.colors.bgSurfaceSuccessHover;
};

import i18next from 'i18next';
import { DefaultTheme } from 'styled-components';

/**
 * Normalizes a percentile integer to a 3-tier
 * label system of 'High', 'Med', 'Low' for usability.
 */
export const percentileToLabel = (pct: number) => {
    /* eslint-disable no-else-return */
    if (pct <= 30) {
        return i18next.t('entity.shared.stats:percentile.low');
    } else if (pct > 30 && pct <= 80) {
        return i18next.t('entity.shared.stats:percentile.med');
    }
    return i18next.t('entity.shared.stats:percentile.high');
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

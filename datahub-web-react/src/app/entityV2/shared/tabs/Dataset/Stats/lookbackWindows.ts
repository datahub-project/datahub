import i18next from 'i18next';

import { DateInterval } from '@types';

/**
 * Change this to add or modify the lookback windows that are selectable via the UI.
 */
export const LOOKBACK_WINDOWS = {
    HOUR: {
        get text() {
            return i18next.t('lookbackWindowOptions.hour', { ns: 'entity.profile.stats' });
        },
        windowSize: { interval: DateInterval.Hour, count: 1 },
    },
    DAY: {
        get text() {
            return i18next.t('lookbackWindowOptions.day', { ns: 'entity.profile.stats' });
        },
        windowSize: { interval: DateInterval.Day, count: 1 },
    },
    WEEK: {
        get text() {
            return i18next.t('lookbackWindowOptions.week', { ns: 'entity.profile.stats' });
        },
        windowSize: { interval: DateInterval.Week, count: 1 },
    },
    MONTH: {
        get text() {
            return i18next.t('lookbackWindowOptions.month', { ns: 'entity.profile.stats' });
        },
        windowSize: { interval: DateInterval.Month, count: 1 },
    },
    QUARTER: {
        get text() {
            return i18next.t('lookbackWindowOptions.quarter', { ns: 'entity.profile.stats' });
        },
        windowSize: { interval: DateInterval.Month, count: 3 },
    },
    YEAR: {
        get text() {
            return i18next.t('lookbackWindowOptions.year', { ns: 'entity.profile.stats' });
        },
        windowSize: { interval: DateInterval.Year, count: 1 },
    },
};

export type LookbackWindow = {
    text: string;
    windowSize: {
        interval: DateInterval;
        count: number;
    };
};

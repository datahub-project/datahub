import i18next from 'i18next';

import { AccessTokenDuration } from '@types';

/** Sentinel value for never-expire selections (not an ISO-8601 duration). */
export const ACCESS_TOKEN_NO_EXPIRY = 'NO_EXPIRY';

export const DEFAULT_ACCESS_TOKEN_DURATIONS_ISO = ['PT1H', 'P1D', 'P7D', 'P30D', 'P90D', 'P180D', 'P365D'] as const;

const KNOWN_ISO_LABEL_KEYS: Record<string, string> = {
    PT1H: 'settings.tokens:duration.oneHour',
    P1D: 'settings.tokens:duration.oneDay',
    P7D: 'settings.tokens:duration.oneWeek',
    P30D: 'settings.tokens:duration.oneMonth',
    P1M: 'settings.tokens:duration.oneMonth',
    P90D: 'settings.tokens:duration.threeMonths',
    P3M: 'settings.tokens:duration.threeMonths',
    P180D: 'settings.tokens:duration.sixMonths',
    P6M: 'settings.tokens:duration.sixMonths',
    P365D: 'settings.tokens:duration.oneYear',
    P1Y: 'settings.tokens:duration.oneYear',
};

export type AccessTokenDurationOption = {
    value: string;
    label: string;
};

export function getAccessTokenDurationLabel(isoOrNever: string): string {
    if (isoOrNever === ACCESS_TOKEN_NO_EXPIRY) {
        return i18next.t('settings.tokens:duration.never');
    }
    const key = KNOWN_ISO_LABEL_KEYS[isoOrNever.toUpperCase()];
    if (key) {
        return i18next.t(key);
    }
    return isoOrNever;
}

export function buildAccessTokenDurationOptions(
    allowedIsoDurations: string[],
    allowNoExpiry: boolean,
): AccessTokenDurationOption[] {
    const options: AccessTokenDurationOption[] = allowedIsoDurations.map((iso) => ({
        value: iso,
        label: getAccessTokenDurationLabel(iso),
    }));
    if (allowNoExpiry) {
        options.push({
            value: ACCESS_TOKEN_NO_EXPIRY,
            label: getAccessTokenDurationLabel(ACCESS_TOKEN_NO_EXPIRY),
        });
    }
    return options;
}

export function getDefaultAccessTokenDuration(allowedIsoDurations: string[]): string {
    if (allowedIsoDurations.includes('P30D')) {
        return 'P30D';
    }
    if (allowedIsoDurations.length > 0) {
        return allowedIsoDurations[0];
    }
    return DEFAULT_ACCESS_TOKEN_DURATIONS_ISO[2];
}

const MS_PER_HOUR = 60 * 60 * 1000;
const MS_PER_DAY = 24 * MS_PER_HOUR;

/** Fixed approximations matching backend IsoDurationParser / AccessTokenUtil. */
export function parseAccessTokenDurationToMs(iso: string): number | null {
    const normalized = iso.trim().toUpperCase();
    const match = normalized.match(
        /^P(?!$)(?:(\d+)Y)?(?:(\d+)M)?(?:(\d+)W)?(?:(\d+)D)?(?:T(?=\d)(?:(\d+)H)?(?:(\d+)M)?(?:(\d+(?:\.\d+)?)S)?)?$/,
    );
    if (!match) {
        return null;
    }
    const years = Number(match[1] || 0);
    const months = Number(match[2] || 0);
    const weeks = Number(match[3] || 0);
    const days = Number(match[4] || 0);
    const hours = Number(match[5] || 0);
    const minutes = Number(match[6] || 0);
    const seconds = Number(match[7] || 0);
    const ms =
        years * 365 * MS_PER_DAY +
        months * 30 * MS_PER_DAY +
        weeks * 7 * MS_PER_DAY +
        days * MS_PER_DAY +
        hours * MS_PER_HOUR +
        minutes * 60 * 1000 +
        Math.round(seconds * 1000);
    return ms > 0 ? ms : null;
}

const addMillis = (millis: number) => {
    const result = new Date(Date.now() + millis);
    return i18next.t('settings.tokens:tokenWillExpireOn', {
        date: result.toLocaleDateString(),
        time: result.toLocaleTimeString(),
    });
};

export const getTokenExpireDate = (duration: AccessTokenDuration | string) => {
    if (duration === AccessTokenDuration.NoExpiry || duration === ACCESS_TOKEN_NO_EXPIRY) {
        return i18next.t('settings.tokens:tokenNeverExpires');
    }

    if (typeof duration === 'string') {
        const ms = parseAccessTokenDurationToMs(duration);
        if (ms != null) {
            return addMillis(ms);
        }
    }

    switch (duration) {
        case AccessTokenDuration.OneHour:
            return addMillis(MS_PER_HOUR);
        case AccessTokenDuration.OneDay:
            return addMillis(MS_PER_DAY);
        case AccessTokenDuration.OneWeek:
            return addMillis(7 * MS_PER_DAY);
        case AccessTokenDuration.OneMonth:
            return addMillis(30 * MS_PER_DAY);
        case AccessTokenDuration.ThreeMonths:
            return addMillis(90 * MS_PER_DAY);
        case AccessTokenDuration.SixMonths:
            return addMillis(180 * MS_PER_DAY);
        case AccessTokenDuration.OneYear:
            return addMillis(365 * MS_PER_DAY);
        default:
            return i18next.t('settings.tokens:tokenWillExpireOn', {
                date: new Date(Date.now() + 30 * MS_PER_DAY).toLocaleDateString(),
                time: new Date(Date.now() + 30 * MS_PER_DAY).toLocaleTimeString(),
            });
    }
};

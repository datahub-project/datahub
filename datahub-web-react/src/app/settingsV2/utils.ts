import i18next from 'i18next';

import { AccessTokenDuration } from '@types';

/** The duration for which an Access Token is valid. */
export const ACCESS_TOKEN_DURATIONS = [
    {
        get text() {
            return i18next.t('settings.tokens:duration.oneHour');
        },
        duration: AccessTokenDuration.OneHour,
    },
    {
        get text() {
            return i18next.t('settings.tokens:duration.oneDay');
        },
        duration: AccessTokenDuration.OneDay,
    },
    {
        get text() {
            return i18next.t('settings.tokens:duration.oneMonth');
        },
        duration: AccessTokenDuration.OneMonth,
    },
    {
        get text() {
            return i18next.t('settings.tokens:duration.threeMonths');
        },
        duration: AccessTokenDuration.ThreeMonths,
    },
    {
        get text() {
            return i18next.t('settings.tokens:duration.never');
        },
        duration: AccessTokenDuration.NoExpiry,
    },
];

const addHours = (hour: number) => {
    const result = new Date();
    result.setHours(result.getHours() + hour);
    return i18next.t('settings.tokens:tokenWillExpireOn', {
        date: result.toLocaleDateString(),
        time: result.toLocaleTimeString(),
    });
};

const addDays = (days: number) => {
    const result = new Date();
    result.setDate(result.getDate() + days);
    return i18next.t('settings.tokens:tokenWillExpireOn', {
        date: result.toLocaleDateString(),
        time: result.toLocaleTimeString(),
    });
};

export const getTokenExpireDate = (duration: AccessTokenDuration) => {
    switch (duration) {
        case AccessTokenDuration.OneHour:
            return addHours(1);
        case AccessTokenDuration.OneDay:
            return addDays(1);
        case AccessTokenDuration.OneMonth:
            return addDays(30);
        case AccessTokenDuration.ThreeMonths:
            return addDays(90);
        case AccessTokenDuration.NoExpiry:
            return i18next.t('settings.tokens:tokenNeverExpires');
        default:
            return AccessTokenDuration.OneMonth;
    }
};

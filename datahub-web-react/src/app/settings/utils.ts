import { AccessTokenDuration, AccessTokenType } from '../../types.generated';

/** A type of DataHub Access Token. */
export const ACCESS_TOKEN_TYPES = [{ text: 'Personal', type: AccessTokenType.Personal }];

/** The duration for which an Access Token is valid. */
export const ACCESS_TOKEN_DURATIONS = [
    { text: '1 hora', duration: AccessTokenDuration.OneHour },
    { text: '1 dia', duration: AccessTokenDuration.OneDay },
    { text: '1 Mês', duration: AccessTokenDuration.OneMonth },
    { text: '3 Meses', duration: AccessTokenDuration.ThreeMonths },
    { text: 'Nunca', duration: AccessTokenDuration.NoExpiry },
];

const addHours = (hour: number) => {
    const result = new Date();
    result.setHours(result.getHours() + hour);
    return `O token irá expirar em ${result.toLocaleDateString()} no ${result.toLocaleTimeString()}.`;
};

const addDays = (days: number) => {
    const result = new Date();
    result.setDate(result.getDate() + days);
    return `O token irá expirar em ${result.toLocaleDateString()} no ${result.toLocaleTimeString()}.`;
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
            return 'This token will never expire.';
        default:
            return AccessTokenDuration.OneMonth;
    }
};

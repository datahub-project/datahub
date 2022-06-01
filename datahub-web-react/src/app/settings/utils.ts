import { AccessTokenDuration, AccessTokenType } from '../../types.generated';

/** A type of DataHub Access Token. */
export const ACCESS_TOKEN_TYPES = [{ text: 'Personal', type: AccessTokenType.Personal }];

/** The duration for which an Access Token is valid. */
export const ACCESS_TOKEN_DURATIONS = [
    { text: '1 hour', duration: AccessTokenDuration.OneHour },
    { text: '1 day', duration: AccessTokenDuration.OneDay },
    { text: '1 month', duration: AccessTokenDuration.OneMonth },
    { text: '3 months', duration: AccessTokenDuration.ThreeMonths },
    { text: '6 months', duration: AccessTokenDuration.SixMonths },
];

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { AccessTokenDuration, AccessTokenType } from '@types';

/** A type of DataHub Access Token. */
export const ACCESS_TOKEN_TYPES = [{ text: 'Personal', type: AccessTokenType.Personal }];

/** The duration for which an Access Token is valid. */
export const ACCESS_TOKEN_DURATIONS = [
    { text: '1 hour', duration: AccessTokenDuration.OneHour },
    { text: '1 day', duration: AccessTokenDuration.OneDay },
    { text: '1 month', duration: AccessTokenDuration.OneMonth },
    { text: '3 months', duration: AccessTokenDuration.ThreeMonths },
    { text: 'Never', duration: AccessTokenDuration.NoExpiry },
];

const addHours = (hour: number) => {
    const result = new Date();
    result.setHours(result.getHours() + hour);
    return `The token will expire on ${result.toLocaleDateString()} at ${result.toLocaleTimeString()}.`;
};

const addDays = (days: number) => {
    const result = new Date();
    result.setDate(result.getDate() + days);
    return `The token will expire on ${result.toLocaleDateString()} at ${result.toLocaleTimeString()}.`;
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

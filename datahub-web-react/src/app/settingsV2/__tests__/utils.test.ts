import { describe, expect, it } from 'vitest';

import { ACCESS_TOKEN_DURATIONS, getTokenExpireDate } from '@app/settingsV2/utils';

import { AccessTokenDuration } from '@types';

describe('settingsV2 utils', () => {
    describe('ACCESS_TOKEN_DURATIONS', () => {
        it('should include all supported durations', () => {
            const durations = ACCESS_TOKEN_DURATIONS.map((d) => d.duration);
            expect(durations).toContain(AccessTokenDuration.OneHour);
            expect(durations).toContain(AccessTokenDuration.OneDay);
            expect(durations).toContain(AccessTokenDuration.OneMonth);
            expect(durations).toContain(AccessTokenDuration.ThreeMonths);
            expect(durations).toContain(AccessTokenDuration.NoExpiry);
        });

        it('should have a text getter for each entry', () => {
            ACCESS_TOKEN_DURATIONS.forEach((entry) => {
                expect(entry.text).toBeTruthy();
            });
        });
    });

    describe('getTokenExpireDate', () => {
        it('should return a non-expiry string for NoExpiry', () => {
            const result = getTokenExpireDate(AccessTokenDuration.NoExpiry);
            expect(result).toBe('This token will never expire.');
        });

        it('should return a date string for OneHour', () => {
            const result = getTokenExpireDate(AccessTokenDuration.OneHour);
            expect(typeof result).toBe('string');
            expect(result).toContain('expire');
        });

        it('should return a date string for OneDay', () => {
            const result = getTokenExpireDate(AccessTokenDuration.OneDay);
            expect(typeof result).toBe('string');
            expect(result).toContain('expire');
        });

        it('should return a date string for OneMonth', () => {
            const result = getTokenExpireDate(AccessTokenDuration.OneMonth);
            expect(typeof result).toBe('string');
            expect(result).toContain('expire');
        });

        it('should return a date string for ThreeMonths', () => {
            const result = getTokenExpireDate(AccessTokenDuration.ThreeMonths);
            expect(typeof result).toBe('string');
            expect(result).toContain('expire');
        });

        it('should return default (OneMonth duration) for unknown duration', () => {
            const result = getTokenExpireDate('UNKNOWN_DURATION' as AccessTokenDuration);
            expect(result).toBe(AccessTokenDuration.OneMonth);
        });
    });
});

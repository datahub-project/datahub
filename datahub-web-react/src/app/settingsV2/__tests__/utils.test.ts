import { describe, expect, it } from 'vitest';

import {
    ACCESS_TOKEN_NO_EXPIRY,
    buildAccessTokenDurationOptions,
    getDefaultAccessTokenDuration,
    getTokenExpireDate,
    parseAccessTokenDurationToMs,
} from '@app/settingsV2/utils';

import { AccessTokenDuration } from '@types';

describe('settingsV2 utils', () => {
    describe('buildAccessTokenDurationOptions', () => {
        it('should map ISO durations and omit Never when disallowed', () => {
            const options = buildAccessTokenDurationOptions(['PT1H', 'P30D'], false);
            expect(options.map((o) => o.value)).toEqual(['PT1H', 'P30D']);
        });

        it('should append Never when allowNoExpiry is true', () => {
            const options = buildAccessTokenDurationOptions(['P30D'], true);
            expect(options.map((o) => o.value)).toEqual(['P30D', ACCESS_TOKEN_NO_EXPIRY]);
        });
    });

    describe('getDefaultAccessTokenDuration', () => {
        it('should prefer P30D when present', () => {
            expect(getDefaultAccessTokenDuration(['PT1H', 'P30D', 'P365D'])).toBe('P30D');
        });

        it('should fall back to the first allowed duration', () => {
            expect(getDefaultAccessTokenDuration(['PT1H', 'P1D'])).toBe('PT1H');
        });
    });

    describe('parseAccessTokenDurationToMs', () => {
        it('should parse known durations', () => {
            expect(parseAccessTokenDurationToMs('PT1H')).toBe(3_600_000);
            expect(parseAccessTokenDurationToMs('P30D')).toBe(2_592_000_000);
            expect(parseAccessTokenDurationToMs('P1Y')).toBe(31_536_000_000);
        });
    });

    describe('getTokenExpireDate', () => {
        it('should return a non-expiry string for NoExpiry', () => {
            const result = getTokenExpireDate(AccessTokenDuration.NoExpiry);
            expect(result).toBe('This token will never expire.');
        });

        it('should return a non-expiry string for NO_EXPIRY sentinel', () => {
            expect(getTokenExpireDate(ACCESS_TOKEN_NO_EXPIRY)).toBe('This token will never expire.');
        });

        it('should return a date string for OneHour', () => {
            const result = getTokenExpireDate(AccessTokenDuration.OneHour);
            expect(typeof result).toBe('string');
            expect(result).toContain('expire');
        });

        it('should return a date string for ISO durations', () => {
            const result = getTokenExpireDate('P30D');
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

        it('should return a fallback expiry message for unknown duration', () => {
            const result = getTokenExpireDate('UNKNOWN_DURATION' as AccessTokenDuration);
            expect(typeof result).toBe('string');
            expect(result).toContain('expire');
        });
    });
});

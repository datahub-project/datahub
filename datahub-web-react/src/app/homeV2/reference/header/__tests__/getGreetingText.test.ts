import { beforeEach, describe, expect, it, vi } from 'vitest';

import { getGreetingText } from '@app/homeV2/reference/header/getGreetingText';

vi.mock('i18next', () => ({
    default: {
        t: (key: string) => key,
    },
}));

describe('getGreetingText', () => {
    beforeEach(() => {
        vi.useRealTimers();
    });

    it('returns morning greeting before noon', () => {
        vi.setSystemTime(new Date('2024-01-01T08:00:00'));
        expect(getGreetingText()).toBe('home.v2:greeting.morning');
    });

    it('returns afternoon greeting from noon to 17:00', () => {
        vi.setSystemTime(new Date('2024-01-01T14:00:00'));
        expect(getGreetingText()).toBe('home.v2:greeting.afternoon');
    });

    it('returns evening greeting from 17:00 onwards', () => {
        vi.setSystemTime(new Date('2024-01-01T20:00:00'));
        expect(getGreetingText()).toBe('home.v2:greeting.evening');
    });

    it('returns morning greeting at midnight', () => {
        vi.setSystemTime(new Date('2024-01-01T00:00:00'));
        expect(getGreetingText()).toBe('home.v2:greeting.morning');
    });

    it('returns afternoon greeting at exactly noon', () => {
        vi.setSystemTime(new Date('2024-01-01T12:00:00'));
        expect(getGreetingText()).toBe('home.v2:greeting.afternoon');
    });

    it('returns evening greeting at exactly 17:00', () => {
        vi.setSystemTime(new Date('2024-01-01T17:00:00'));
        expect(getGreetingText()).toBe('home.v2:greeting.evening');
    });
});

import cronstrue from 'cronstrue';
import i18next from 'i18next';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { cronToString, removeTimePrefix } from '@utils/cronstrue';

vi.mock('cronstrue', () => ({
    default: { toString: vi.fn() },
}));

vi.mock('i18next', () => ({
    default: { language: 'en' },
}));

describe('cronToString', () => {
    beforeEach(() => {
        vi.clearAllMocks();
        (i18next as any).language = 'en';
    });

    it('passes the current i18next language as locale', () => {
        (i18next as any).language = 'de';
        vi.mocked(cronstrue.toString).mockReturnValue('Um 09:00 Uhr');

        cronToString('0 9 * * *');

        expect(cronstrue.toString).toHaveBeenCalledWith('0 9 * * *', { locale: 'de' });
    });

    it('forwards additional options alongside the locale', () => {
        vi.mocked(cronstrue.toString).mockReturnValue('At 09:00');

        cronToString('0 9 * * 1-5', { use24HourTimeFormat: true });

        expect(cronstrue.toString).toHaveBeenCalledWith('0 9 * * 1-5', {
            use24HourTimeFormat: true,
            locale: 'en',
        });
    });

    it('returns the value from cronstrue', () => {
        vi.mocked(cronstrue.toString).mockReturnValue('At 9:00 AM');

        expect(cronToString('0 9 * * *')).toBe('At 9:00 AM');
    });
});

describe('removeTimePrefix', () => {
    beforeEach(() => {
        (i18next as any).language = 'en';
    });

    it('removes "At " prefix in English', () => {
        expect(removeTimePrefix('At 9:00 AM')).toBe('9:00 AM');
    });

    it('matches prefix case-insensitively in English', () => {
        expect(removeTimePrefix('at 9:00 AM')).toBe('9:00 AM');
    });

    it('removes "Um " prefix in German', () => {
        (i18next as any).language = 'de';
        expect(removeTimePrefix('Um 9:00 Uhr')).toBe('9:00 Uhr');
    });

    it('returns the string unchanged for an unsupported language', () => {
        (i18next as any).language = 'fr';
        expect(removeTimePrefix('À 9:00')).toBe('À 9:00');
    });

    it('returns the string unchanged when the prefix is absent', () => {
        expect(removeTimePrefix('Every minute')).toBe('Every minute');
    });
});

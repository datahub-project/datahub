import { addInterval, getSupportedTimezones } from '@app/shared/time/timeUtils';

import { DateInterval } from '@types';

describe('timeUtils', () => {
    describe('addInterval', () => {
        it('add date interval works correctly', () => {
            const input = new Date(1677242304000);
            const afterAdd = addInterval(1, input, DateInterval.Month);
            const expected = new Date(1679661504000);
            expect(afterAdd.getTime()).toEqual(expected.getTime());
        });
    });

    describe('getSupportedTimezones', () => {
        it('includes common canonical timezones from Intl', () => {
            const timezones = getSupportedTimezones();

            expect(timezones).toContain('America/New_York');
            expect(timezones).toContain('America/Chicago');
            expect(timezones).toContain('America/Los_Angeles');
            expect(timezones).toContain('Europe/London');
            expect(timezones).toContain('Asia/Tokyo');
            expect(timezones).toContain('Asia/Singapore');
            expect(timezones).toContain('Asia/Calcutta');
        });

        it('includes common moment-timezone aliases supported by Java ZoneId', () => {
            const timezones = getSupportedTimezones();

            expect(timezones).toContain('Asia/Kolkata');
            expect(timezones).toContain('CET');
            expect(timezones).toContain('UTC');
            expect(timezones).toContain('Etc/UTC');
            expect(timezones).toContain('GMT');
            expect(timezones).toContain('Etc/GMT');
            expect(timezones).toContain('US/Eastern');
            expect(timezones).toContain('US/Central');
            expect(timezones).toContain('US/Pacific');
        });

        it('does not add less common moment-timezone aliases', () => {
            const timezones = getSupportedTimezones();

            expect(timezones).not.toContain('Europe/Kyiv');
            expect(timezones).not.toContain('Canada/Eastern');
            expect(timezones).not.toContain('PST8PDT');
            expect(timezones).not.toContain('IST');
        });
    });
});

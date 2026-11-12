import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { LOOKBACK_WINDOWS } from '@app/entityV2/shared/tabs/Dataset/Stats/lookbackWindows';
import {
    calculateInitialLookbackWindowFromRunEvents,
    getFormattedTimeString,
    getTimeRangeDisplay,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/result/timeline/utils';

const ONE_HOUR_MS = 60 * 60 * 1000;
const ONE_DAY_MS = 24 * ONE_HOUR_MS;
const ONE_YEAR_MS = 365 * ONE_DAY_MS;

const makeEvent = (timestampMillis: number) => ({ __typename: 'AssertionRunEvent', timestampMillis }) as any;

// ---------------------------------------------------------------------------
// getTimeRangeDisplay
// ---------------------------------------------------------------------------

describe('getTimeRangeDisplay', () => {
    it('returns "Last hour" for a range of exactly one hour', () => {
        const end = Date.now();
        const start = end - ONE_HOUR_MS;
        expect(getTimeRangeDisplay({ startMs: start, endMs: end })).toBe('Last hour');
    });

    it('returns "Last hour" for a range shorter than one hour', () => {
        const end = Date.now();
        const start = end - 30 * 60 * 1000;
        expect(getTimeRangeDisplay({ startMs: start, endMs: end })).toBe('Last hour');
    });

    it('returns "Last day" for a range of exactly one day', () => {
        const end = Date.now();
        const start = end - ONE_DAY_MS;
        expect(getTimeRangeDisplay({ startMs: start, endMs: end })).toBe('Last day');
    });

    it('returns "Last day" for a range between one hour and one day', () => {
        const end = Date.now();
        const start = end - 6 * ONE_HOUR_MS;
        expect(getTimeRangeDisplay({ startMs: start, endMs: end })).toBe('Last day');
    });

    it('returns "Last year" for a range of exactly one year', () => {
        const end = Date.now();
        const start = end - ONE_YEAR_MS;
        expect(getTimeRangeDisplay({ startMs: start, endMs: end })).toBe('Last year');
    });

    it('returns "Last year" for a range longer than one year', () => {
        const end = Date.now();
        const start = end - 2 * ONE_YEAR_MS;
        expect(getTimeRangeDisplay({ startMs: start, endMs: end })).toBe('Last year');
    });

    it('returns a formatted date range for a range between one day and one year', () => {
        const startMs = new Date('2024-01-01T00:00:00.000Z').getTime();
        const endMs = new Date('2024-06-01T00:00:00.000Z').getTime();
        const startDate = new Date(startMs).toLocaleDateString('en-us', { month: 'short', day: 'numeric' });
        const endDate = new Date(endMs).toLocaleDateString('en-us', { month: 'short', day: 'numeric' });
        expect(getTimeRangeDisplay({ startMs, endMs })).toBe(`${startDate} - ${endDate}`);
    });
});

// ---------------------------------------------------------------------------
// getFormattedTimeString
// ---------------------------------------------------------------------------

describe('getFormattedTimeString', () => {
    const FIXED_NOW = new Date('2024-06-15T12:00:00.000Z').getTime();

    beforeEach(() => {
        vi.useFakeTimers();
        vi.setSystemTime(FIXED_NOW);
    });

    afterEach(() => {
        vi.useRealTimers();
    });

    it('shows time of day (HH:MM AM/PM) for a timestamp less than 1 day ago', () => {
        const ts = FIXED_NOW - 4 * ONE_HOUR_MS;
        expect(getFormattedTimeString(ts)).toBe(
            new Date(ts).toLocaleTimeString('en-us', { hour: '2-digit', minute: '2-digit' }),
        );
    });

    it('shows month and day for a timestamp between 1 day and 1 year ago', () => {
        const ts = FIXED_NOW - 30 * ONE_DAY_MS;
        expect(getFormattedTimeString(ts)).toBe(
            new Date(ts).toLocaleDateString('en-us', { month: 'short', day: 'numeric' }),
        );
    });

    it('shows month and year for a timestamp more than 1 year ago', () => {
        const ts = FIXED_NOW - 2 * ONE_YEAR_MS;
        expect(getFormattedTimeString(ts)).toBe(
            new Date(ts).toLocaleDateString('en-us', { month: 'short', year: 'numeric' }),
        );
    });

    it('shows time of day at the boundary (just under 1 day ago)', () => {
        const ts = FIXED_NOW - ONE_DAY_MS + 1;
        expect(getFormattedTimeString(ts)).toBe(
            new Date(ts).toLocaleTimeString('en-us', { hour: '2-digit', minute: '2-digit' }),
        );
    });

    it('shows month and day at the boundary (just under 1 year ago)', () => {
        const ts = FIXED_NOW - ONE_YEAR_MS + 1;
        expect(getFormattedTimeString(ts)).toBe(
            new Date(ts).toLocaleDateString('en-us', { month: 'short', day: 'numeric' }),
        );
    });
});

// ---------------------------------------------------------------------------
// calculateInitialLookbackWindowFromRunEvents
// ---------------------------------------------------------------------------

describe('calculateInitialLookbackWindowFromRunEvents', () => {
    it('returns undefined for an empty array', () => {
        expect(calculateInitialLookbackWindowFromRunEvents([])).toBeUndefined();
    });

    it('returns undefined when there is only one event', () => {
        expect(calculateInitialLookbackWindowFromRunEvents([makeEvent(1000)])).toBeUndefined();
    });

    it('returns MONTH when the gap between the two latest events exceeds 3.5 days', () => {
        const now = Date.now();
        const events = [makeEvent(now), makeEvent(now - 4 * ONE_DAY_MS)];
        expect(calculateInitialLookbackWindowFromRunEvents(events)).toBe(LOOKBACK_WINDOWS.MONTH);
    });

    it('returns WEEK when the gap is between 12 hours and 3.5 days', () => {
        const now = Date.now();
        const events = [makeEvent(now), makeEvent(now - ONE_DAY_MS)];
        expect(calculateInitialLookbackWindowFromRunEvents(events)).toBe(LOOKBACK_WINDOWS.WEEK);
    });

    it('returns DAY when the gap is between 30 minutes and 12 hours', () => {
        const now = Date.now();
        const events = [makeEvent(now), makeEvent(now - ONE_HOUR_MS)];
        expect(calculateInitialLookbackWindowFromRunEvents(events)).toBe(LOOKBACK_WINDOWS.DAY);
    });

    it('returns HOUR when the gap is 30 minutes or less', () => {
        const now = Date.now();
        const events = [makeEvent(now), makeEvent(now - 15 * 60 * 1000)];
        expect(calculateInitialLookbackWindowFromRunEvents(events)).toBe(LOOKBACK_WINDOWS.HOUR);
    });

    it('uses Math.abs so event order does not affect the result', () => {
        const now = Date.now();
        const older = now - 4 * ONE_DAY_MS;
        expect(calculateInitialLookbackWindowFromRunEvents([makeEvent(now), makeEvent(older)])).toBe(
            LOOKBACK_WINDOWS.MONTH,
        );
        expect(calculateInitialLookbackWindowFromRunEvents([makeEvent(older), makeEvent(now)])).toBe(
            LOOKBACK_WINDOWS.MONTH,
        );
    });

    it('ignores events beyond the first two', () => {
        const now = Date.now();
        // First two events are 1 hour apart (→ DAY); third is very old (would be MONTH if used)
        const events = [makeEvent(now), makeEvent(now - ONE_HOUR_MS), makeEvent(now - 10 * ONE_DAY_MS)];
        expect(calculateInitialLookbackWindowFromRunEvents(events)).toBe(LOOKBACK_WINDOWS.DAY);
    });
});

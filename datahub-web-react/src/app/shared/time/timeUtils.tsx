import { DateInterval } from '../../../types.generated';

export const INTERVAL_TO_SECONDS = {
    [DateInterval.Second]: 1,
    [DateInterval.Minute]: 60,
    [DateInterval.Hour]: 3600,
    [DateInterval.Day]: 86400,
    [DateInterval.Week]: 604800,
    [DateInterval.Month]: 2419200,
    [DateInterval.Year]: 31536000,
};

export const INTERVAL_TO_MS = {
    [DateInterval.Second]: 1000,
    [DateInterval.Minute]: 60000,
    [DateInterval.Hour]: 3600000,
    [DateInterval.Day]: 86400000,
    [DateInterval.Week]: 604800000,
    [DateInterval.Month]: 2419200000,
    [DateInterval.Year]: 31536000000,
};

export type TimeWindowSize = {
    interval: DateInterval;
    count: number;
};

export type TimeWindowSizeMs = number;

export type TimeWindow = {
    startTime: number;
    endTime: number;
};

/**
 * Computes the 'width' or 'size' of a fixed time window in milliseconds given a human-readable
 * date interval (ie. day, month, year) + a count (1, 2...).
 *
 * @param interval a human-readable time interval
 * @param count the number of time intervals composing the window
 */
export const getTimeWindowSizeMs = (windowSize: TimeWindowSize): TimeWindowSizeMs => {
    return INTERVAL_TO_SECONDS[windowSize.interval] * 1000 * windowSize.count;
};

/**
 * Computes a time window start time in milliseconds given the end time in milliseconds,
 * an interval representing the time bucket, and an interval count.
 *
 * @param endTimeMillis the end of the window.
 * @param interval the lookback interval (day, month, year)
 * @param count the number of lookback intervals (3).
 */
export const getTimeWindowStart = (endTimeMillis: number, interval: DateInterval, count: number): number => {
    return endTimeMillis - getTimeWindowSizeMs({ interval, count });
};

/**
 * Returns a TimeWindow corresponding to the current time minus a time window of fixed size.
 *
 * @param windowSize the
 */
export const getFixedLookbackWindow = (windowSize: TimeWindowSize): TimeWindow => {
    const endTime = Date.now();
    return {
        startTime: endTime - getTimeWindowSizeMs(windowSize),
        endTime,
    };
};

export const toLocalDateString = (timeMs: number) => {
    const date = new Date(timeMs);
    return date.toLocaleDateString();
};

export const toLocalTimeString = (timeMs: number) => {
    const date = new Date(timeMs);
    return date.toLocaleTimeString();
};

export const toLocalDateTimeString = (timeMs: number) => {
    const date = new Date(timeMs);
    return date.toLocaleString([], {
        year: 'numeric',
        month: 'numeric',
        day: 'numeric',
        hour: '2-digit',
        minute: '2-digit',
        timeZoneName: 'short',
    });
};

export const toUTCDateTimeString = (timeMs: number) => {
    const date = new Date(timeMs);
    return date.toLocaleString([], {
        year: 'numeric',
        month: 'numeric',
        day: 'numeric',
        hour: '2-digit',
        minute: '2-digit',
        timeZone: 'UTC',
        timeZoneName: 'short',
    });
};

export const getLocaleTimezone = () => {
    return Intl.DateTimeFormat().resolvedOptions().timeZone;
};

export const toRelativeTimeString = (timeMs: number) => {
    const rtf = new Intl.RelativeTimeFormat('en', { numeric: 'auto' });

    const diffInMs = timeMs - new Date().getTime();

    const diffInSeconds = Math.round(diffInMs / INTERVAL_TO_MS[DateInterval.Second]);
    if (Math.abs(diffInSeconds) > 0 && Math.abs(diffInSeconds) <= 60) {
        return rtf.format(diffInSeconds, 'second');
    }

    const diffInMinutes = Math.round(diffInMs / INTERVAL_TO_MS[DateInterval.Minute]);
    if (Math.abs(diffInMinutes) > 0 && Math.abs(diffInMinutes) <= 60) {
        return rtf.format(diffInMinutes, 'minute');
    }

    const diffInHours = Math.round(diffInMs / INTERVAL_TO_MS[DateInterval.Hour]);
    if (Math.abs(diffInHours) > 0 && Math.abs(diffInHours) <= 24) {
        return rtf.format(diffInHours, 'hour');
    }

    const diffInDays = Math.round(diffInMs / INTERVAL_TO_MS[DateInterval.Day]);
    if (Math.abs(diffInDays) > 0 && Math.abs(diffInDays) <= 7) {
        return rtf.format(diffInDays, 'day');
    }

    const diffInWeeks = Math.round(diffInMs / INTERVAL_TO_MS[DateInterval.Week]);
    if (Math.abs(diffInWeeks) > 0 && Math.abs(diffInWeeks) <= 4) {
        return rtf.format(diffInWeeks, 'week');
    }

    const diffInMonths = Math.round(diffInMs / INTERVAL_TO_MS[DateInterval.Month]);
    if (Math.abs(diffInMonths) > 0 && Math.abs(diffInMonths) <= 12) {
        return rtf.format(diffInMonths, 'month');
    }

    const diffInYears = Math.round(diffInMs / INTERVAL_TO_MS[DateInterval.Year]);
    return rtf.format(diffInYears, 'year');
};

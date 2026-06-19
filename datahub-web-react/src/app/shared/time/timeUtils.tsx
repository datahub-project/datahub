import i18next from 'i18next';

import dayjs from '@utils/dayjs';
import type { Dayjs, ManipulateType } from '@utils/dayjs';

import { DateInterval } from '@types';

// Intl.supportedValuesOf omits a few common moment-timezone aliases. Keep this list focused on
// customer-facing UTC/GMT variants and common business aliases that Java ZoneId accepts.
const COMMON_TIMEZONE_ALIASES_SUPPORTED_BY_JAVA = [
    'Asia/Kolkata',
    'CET',
    'Etc/GMT',
    'Etc/UTC',
    'GMT',
    'US/Alaska',
    'US/Arizona',
    'US/Central',
    'US/Eastern',
    'US/Hawaii',
    'US/Mountain',
    'US/Pacific',
    'UTC',
];

const INTERVAL_TO_SECONDS = {
    [DateInterval.Second]: 1,
    [DateInterval.Minute]: 60,
    [DateInterval.Hour]: 3600,
    [DateInterval.Day]: 86400,
    [DateInterval.Week]: 604800,
    [DateInterval.Month]: 2419200,
    [DateInterval.Year]: 31536000,
};

const INTERVAL_TO_MS = {
    [DateInterval.Second]: 1000,
    [DateInterval.Minute]: 60000,
    [DateInterval.Hour]: 3600000,
    [DateInterval.Day]: 86400000,
    [DateInterval.Week]: 604800000,
    [DateInterval.Month]: 2419200000,
    [DateInterval.Year]: 31536000000,
};

const INTERVAL_TO_DURATION_UNIT: { [key: string]: ManipulateType } = {
    [DateInterval.Second]: 'seconds',
    [DateInterval.Minute]: 'minutes',
    [DateInterval.Hour]: 'hours',
    [DateInterval.Day]: 'days',
    [DateInterval.Week]: 'weeks',
    [DateInterval.Month]: 'months',
    [DateInterval.Year]: 'years',
};

export type TimeWindowSize = {
    interval: DateInterval;
    count: number;
};

type TimeWindowSizeMs = number;

type TimeWindow = {
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
const getTimeWindowSizeMs = (windowSize: TimeWindowSize): TimeWindowSizeMs => {
    return INTERVAL_TO_SECONDS[windowSize.interval] * 1000 * windowSize.count;
};

export const addInterval = (interval_num: number, date: Date, interval: DateInterval): Date => {
    return dayjs(date).utc().add(interval_num, INTERVAL_TO_DURATION_UNIT[interval]).toDate();
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
    const endTime = dayjs().valueOf();
    const startTime = dayjs(endTime)
        .subtract(windowSize.count, INTERVAL_TO_DURATION_UNIT[windowSize.interval])
        .valueOf();

    return {
        startTime,
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

export const getLocaleTimezone = () => {
    return Intl.DateTimeFormat().resolvedOptions().timeZone;
};

export const getSupportedTimezones = () => {
    const intlWithSupportedValues = Intl as unknown as { supportedValuesOf?: (input: string) => string[] };
    const timezones = intlWithSupportedValues.supportedValuesOf?.('timeZone') || [];

    return Array.from(new Set([...timezones, ...COMMON_TIMEZONE_ALIASES_SUPPORTED_BY_JAVA])).sort();
};

export const toRelativeTimeString = (timeMs: number | undefined) => {
    const rtf = new Intl.RelativeTimeFormat(i18next.language || 'en', { numeric: 'auto' });

    if (!timeMs) return null;
    const diffInMs = timeMs - new Date().getTime();

    const diffInSeconds = Math.round(diffInMs / INTERVAL_TO_MS[DateInterval.Second]);

    if (Math.abs(diffInSeconds) === 0) {
        return i18next.t('shared.time:justNow');
    }

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

export function getTimeFromNow(timestampMillis) {
    if (!timestampMillis) {
        return '';
    }
    const relativeTimeString = dayjs(timestampMillis).fromNow();
    if (relativeTimeString === 'a few seconds ago' /* untranslated-text -- dayjs library output comparison value */) {
        return i18next.t('shared.time:now');
    }
    return relativeTimeString;
}

export function getTimeRangeDescription(startDate: Dayjs | null, endDate: Dayjs | null): string {
    if (!startDate && !endDate) {
        return i18next.t('shared.time:allTime');
    }

    if (!startDate && endDate) {
        return i18next.t('shared.time:untilDate', { date: endDate.format('ll') });
    }

    if (startDate && !endDate) {
        return i18next.t('shared.time:fromDate', { date: startDate.format('ll') });
    }

    if (startDate && endDate) {
        if (endDate && endDate.isSame(dayjs(), 'day')) {
            const startDateRelativeTime = dayjs().diff(startDate, 'days');
            return i18next.t('shared.time:lastDaysCount', { count: startDateRelativeTime });
        }

        if (endDate.isSame(startDate, 'day')) {
            return startDate.format('ll');
        }
        return i18next.t('shared.time:dateRange', {
            startDate: startDate.format('ll'),
            endDate: endDate.format('ll'),
        });
    }

    return i18next.t('shared.time:unknownTimeRange');
}

export function formatDuration(durationMs: number): string {
    const duration = dayjs.duration(durationMs);
    const hours = Math.floor(duration.asHours());
    const minutes = duration.minutes();
    const seconds = duration.seconds();

    if (hours === 0 && minutes === 0) {
        return i18next.t('shared.time:durationSecondsCount', { count: seconds });
    }

    if (hours === 0) {
        return i18next.t('shared.time:durationMinutesCount', { count: minutes });
    }

    const hourStr = i18next.t('shared.time:durationHoursCount', { count: hours });
    const minuteStr = minutes > 0 ? ` ${i18next.t('shared.time:durationMinutesCount', { count: minutes })}` : '';
    return `${hourStr}${minuteStr}`;
}

export function formatDetailedDuration(durationMs: number): string {
    const duration = dayjs.duration(durationMs);
    const hours = Math.floor(duration.asHours());
    const minutes = duration.minutes();
    const seconds = duration.seconds();

    const parts: string[] = [];

    if (hours > 0) {
        parts.push(i18next.t('shared.time:durationHoursCount', { count: hours }));
    }
    if (minutes > 0) {
        parts.push(i18next.t('shared.time:durationMinutesCount', { count: minutes }));
    }
    if (seconds > 0) {
        parts.push(i18next.t('shared.time:durationSecondsCount', { count: seconds }));
    }
    return parts.join(' ');
}

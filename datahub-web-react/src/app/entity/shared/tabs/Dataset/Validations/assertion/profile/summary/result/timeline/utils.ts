import { Maybe } from 'graphql/jsutils/Maybe';
import { AssertionInfo, AssertionResultType, AssertionRunEvent, AssertionStdOperator, AssertionStdParameterType, AssertionStdParameters, AssertionType, FieldAssertionType, FreshnessAssertionScheduleType, FreshnessAssertionType, VolumeAssertionType } from '../../../../../../../../../../../types.generated';
import { AssertionDataPoint } from './types';

const ONE_HOUR_IN_MS = 60 * 60 * 1000;
const ONE_DAY_IN_MS = 24 * ONE_HOUR_IN_MS;

export function isLessThanOneDay(timeRange) {
    return timeRange.endMs - timeRange.startMs <= ONE_DAY_IN_MS;
}

export function isLessThanOneHour(timeRange) {
    return timeRange.endMs - timeRange.startMs <= ONE_HOUR_IN_MS;
}

export function isGreaterThanOneYear(timeRange) {
    return timeRange.endMs - timeRange.startMs >= 365 * ONE_DAY_IN_MS;
}

export function getTimeRangeDisplay(timeRange) {
    /**
     * Start and end dates being observed in the chart.
     */
    const startDate = new Date(timeRange.startMs).toLocaleDateString('en-us', {
        month: 'short',
        day: 'numeric',
    });
    const endDate = new Date(timeRange.endMs).toLocaleDateString('en-us', {
        month: 'short',
        day: 'numeric',
    });

    if (isLessThanOneHour(timeRange)) {
        return 'Last hour';
    }
    if (isLessThanOneDay(timeRange)) {
        return 'Last day';
    }
    if (isGreaterThanOneYear(timeRange)) {
        return 'Last year';
    }
    return `${startDate} - ${endDate}`;
}

export const getFormattedTimeString = (timestampMs) => {
    // If the time is less than 1 day ago, show the time of day in hours and minutes.
    if (Date.now() - timestampMs < ONE_DAY_IN_MS) {
        return new Date(timestampMs).toLocaleTimeString('en-us', { hour: '2-digit', minute: '2-digit' });
    }
    // If the time is less than 1 year ago, show the date in month and day.
    if (Date.now() - timestampMs < 365 * ONE_DAY_IN_MS) {
        return new Date(timestampMs).toLocaleDateString('en-us', { month: 'short', day: 'numeric' });
    }
    // If the time is more than 1 year ago, show the date in month and year.
    return new Date(timestampMs).toLocaleDateString('en-us', { month: 'short', year: 'numeric' });
};

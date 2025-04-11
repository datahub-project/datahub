import { AssertionRunEventDetailsFragment } from '../../../../../../../../../../../graphql/assertion.generated';
import { LOOKBACK_WINDOWS, LookbackWindow } from '../../../../../../Stats/lookbackWindows';

const ONE_HOUR_IN_MS = 60 * 60 * 1000; // Milliseconds in one hour
const ONE_DAY_IN_MS = 24 * ONE_HOUR_IN_MS; // Milliseconds in one day
const ONE_WEEK_IN_MS = 7 * ONE_DAY_IN_MS; // Milliseconds in one week

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

export function calculateInitialLookbackWindowFromRunEvents(
    allRunEvents: Array<{ __typename?: 'AssertionRunEvent' } & AssertionRunEventDetailsFragment>,
): LookbackWindow | undefined {
    if (!allRunEvents?.length) return undefined;

    // Take the latest two events.
    // A more fancy way would be to recency-weighted averages across the board...
    // ...but the most recent are almost always indicative of currently relevant time window, so we keep it simple for now.
    const ts1 = allRunEvents[0].timestampMillis;
    const ts2 = allRunEvents[1] ? allRunEvents[1].timestampMillis : undefined;

    if (typeof ts2 !== 'number') return undefined;

    const diffMillis = Math.abs(ts1 - ts2);

    const ticksPerAtom = 2;
    const adjustmentFactor = 1 / ticksPerAtom;

    // If the difference between the two ts is larger than (ie.) 1wk, we set the window to 1mo
    // or if the difference is 1h, we set the window to a day
    // The above 'adjustment' factor basically lets us add some tolerance, so instead of 1wk, if the difference is 1/2wk, we still roll up to 1mo.
    let lookbackWindow: LookbackWindow | undefined;
    if (diffMillis / (ONE_WEEK_IN_MS * adjustmentFactor) > 1) {
        lookbackWindow = LOOKBACK_WINDOWS.MONTH;
    } else if (diffMillis / (ONE_DAY_IN_MS * adjustmentFactor) > 1) {
        lookbackWindow = LOOKBACK_WINDOWS.WEEK;
    } else if (diffMillis / (ONE_HOUR_IN_MS * adjustmentFactor) > 1) {
        lookbackWindow = LOOKBACK_WINDOWS.DAY;
    } else {
        lookbackWindow = LOOKBACK_WINDOWS.HOUR;
    }
    return lookbackWindow;
}

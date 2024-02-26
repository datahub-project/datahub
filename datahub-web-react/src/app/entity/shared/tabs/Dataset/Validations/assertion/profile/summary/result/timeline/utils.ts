import { AssertionResultType, AssertionRunEvent } from '../../../../../../../../../../../types.generated';
import { AssertionDataPoint } from './AssertionResultTimelineChart';

export const SUCCESS_COLOR_HEX = '#52C41A';
export const FAILURE_COLOR_HEX = '#F5222D';
export const ERROR_COLOR_HEX = '#FAAD14';
export const INIT_COLOR_HEX = '#8C8C8C';

export const getFillColor = (type: AssertionResultType) => {
    switch (type) {
        case AssertionResultType.Success:
            return SUCCESS_COLOR_HEX;
        case AssertionResultType.Failure:
            return FAILURE_COLOR_HEX;
        case AssertionResultType.Error:
            return ERROR_COLOR_HEX;
        case AssertionResultType.Init:
            return INIT_COLOR_HEX;
        default:
            throw new Error(`Unsupported Assertion Result Type ${type} provided.`);
    }
};

export function generateTickValues(startMs, endMs): Date[] {
    const ticks: Date[] = [];
    const minDate = new Date(startMs);
    const maxDate = new Date(endMs);
    const diff = maxDate.getTime() - minDate.getTime();

    const oneHourInMillis = 60 * 60 * 1000; // Milliseconds in one hour
    const oneDayInMillis = 24 * oneHourInMillis; // Milliseconds in one day
    const oneWeekInMillis = 7 * oneDayInMillis; // Milliseconds in one week

    if (diff <= oneHourInMillis) {
        // Last hour or less: ticks every 15 minutes
        const currentDate = new Date(Math.floor(minDate.getTime() / oneHourInMillis) * oneHourInMillis);
        while (currentDate <= maxDate) {
            ticks.push(new Date(currentDate));
            currentDate.setMinutes(currentDate.getMinutes() + 15);
        }
    } else if (diff <= oneDayInMillis) {
        // Last day or less: ticks every 4 hours
        const currentDate = new Date(minDate);
        currentDate.setHours(minDate.getHours() - (minDate.getHours() % 4), 0, 0, 0); // Align to the nearest 4-hour mark
        while (currentDate <= maxDate) {
            ticks.push(new Date(currentDate));
            currentDate.setHours(currentDate.getHours() + 4);
        }
    } else if (diff <= oneWeekInMillis) {
        // Less than a week: ticks at the start of each day
        const currentDate = new Date(minDate);
        currentDate.setHours(0, 0, 0, 0); // Start of the day
        while (currentDate <= maxDate) {
            ticks.push(new Date(currentDate));
            currentDate.setDate(currentDate.getDate() + 1);
        }
    } else {
        // More than a week: divide the range into 8 equal parts
        const interval = diff / 7; // Divide into 8 parts, but calculate interval based on 7 divisions to include both start and end points
        const currentDate = new Date(minDate.getTime());
        for (let i = 0; i <= 7; i++) {
            // Ensure we add the last tick by iterating one extra time
            ticks.push(new Date(currentDate.getTime() + i * interval));
        }
    }

    return ticks;
}

export function getCustomTickValue(v, timeRange) {
    // Assuming 'v' is a JavaScript Date object.
    // You'll need to adjust 'minDate' and 'maxDate' to reflect your actual data range.
    const minDate = new Date(timeRange.startMs);
    const maxDate = new Date(timeRange.endMs);

    const oneHourInMillis = 60 * 60 * 1000; // Milliseconds in one hour
    const oneDayInMillis = 24 * 60 * 60 * 1000; // Milliseconds in one day

    // Determine if the range is less than or equal to one day
    if (maxDate.getTime() - minDate.getTime() <= oneHourInMillis) {
        return v.toLocaleTimeString('en-us', { hour: '2-digit', minute: '2-digit' });
    }
    if (maxDate.getTime() - minDate.getTime() <= oneDayInMillis) {
        // Format as hour of the day if the range is within a single day
        return v.toLocaleTimeString('en-us', { hour: '2-digit' });
    }
    // Format as month and day if the range is more than one day
    return v.toLocaleDateString('en-us', { month: 'short', day: 'numeric' });
}

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

export const getAssertionDataPointsFromRunEvents = (runEvents: AssertionRunEvent[], options?: {
    getPopOverContent?: (runEvent: AssertionRunEvent) => JSX.Element | undefined
    getTitleElement?: (runEvent: AssertionRunEvent) => JSX.Element | undefined,
}): AssertionDataPoint[] => {
    return runEvents
        .filter((runEvent) => !!runEvent.result)
        .map((runEvent) => {
            const { result } = runEvent;
            if (!result) throw new Error('Completed assertion run event does not have a result.');
            const resultUrl = result.externalUrl;
            /**
             * Create a "result" to render in the timeline chart.
             */
            return {
                time: runEvent.timestampMillis,
                result: {
                    type: result.type,
                    resultUrl,
                    title: options?.getTitleElement?.(runEvent),
                    popoverContent: options?.getPopOverContent?.(runEvent),
                },
            };
        }) || [];
}
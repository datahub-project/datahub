import _ from 'lodash';

import {
    AssertionInfo,
    AssertionResultType,
    AssertionRunEvent,
    AssertionType,
    FreshnessAssertionInfo,
    FreshnessAssertionScheduleType,
    Maybe,
} from '../../../../../../../../../../../../types.generated';
import { INTERVAL_TO_MS } from '../../../../../../../../../../../shared/time/timeUtils';
import { tryGetExpectedRangeFromAssertionRunEvent } from '../../../shared/resultExtractionUtils';
import { AssertionChartType, AssertionDataPoint } from './types';
import { getPreviousScheduleEvaluationTimeMs } from '../../../../../../acrylUtils';
import { ASSERTION_NATIVE_RESULTS_KEYS_BY_ASSERTION_TYPE } from '../../../shared/constants';

export const ACCENT_COLOR_HEX = '#222222';
export const EXTRA_HIGHLIGHT_COLOR_HEX = '#4050E7';
export const SUCCESS_COLOR_HEX = '#52C41A';
export const FAILURE_COLOR_HEX = '#F5222D';
export const ERROR_COLOR_HEX = '#FAAD14';
export const INIT_COLOR_HEX = '#8C8C8C';
export const EXPECTED_RANGE_SHADE_COLOR = '#11d469';

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

/**
 * For now we're keeping it simple by only showing the range of values being displayed on the chart
 * @param minY
 * @param maxY
 * @returns {number[]}
 */
export function generateYScaleTickValues(minY: number, maxY: number): number[] {
    return [minY, maxY];
}

export function generateTimeScaleTickValues(startMs: number, endMs: number): Date[] {
    const ticks: Date[] = [];
    const minDate = new Date(startMs);
    const maxDate = new Date(endMs);
    const diff = maxDate.getTime() - minDate.getTime();

    const oneHourInMillis = 60 * 60 * 1000; // Milliseconds in one hour
    const oneDayInMillis = 24 * oneHourInMillis; // Milliseconds in one day
    const oneWeekInMillis = 7 * oneDayInMillis; // Milliseconds in one week

    if (diff <= oneHourInMillis) {
        // Last hour or less: ticks every 15 minutes
        const currentDate = new Date(minDate);
        while (currentDate <= maxDate) {
            ticks.push(new Date(currentDate));
            currentDate.setMinutes(currentDate.getMinutes() + 15);
        }
    } else if (diff <= oneDayInMillis) {
        // Last day or less: ticks every 4 hours
        const currentDate = new Date(minDate);
        currentDate.setHours(minDate.getHours() - (minDate.getHours() % 4), 0, 0, 0); // Align to the nearest 4-hour mark
        currentDate.setHours(currentDate.getHours() + 4); // skip hour 0 - looks better on the graph
        while (currentDate <= maxDate) {
            ticks.push(new Date(currentDate));
            currentDate.setHours(currentDate.getHours() + 4);
        }
    } else if (diff <= oneWeekInMillis) {
        // Less than a week: ticks at the start of each day
        const currentDate = new Date(minDate);
        currentDate.setHours(0, 0, 0, 0); // Start of the day
        currentDate.setDate(currentDate.getDate() + 1); // skip day 0 - looks better on the graph
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

export function getCustomTimeScaleTickValue(v, timeRange) {
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
        return v.toLocaleTimeString('en-us', { hour: 'numeric' });
    }
    // Format as month and day if the range is more than one day
    return v.toLocaleDateString('en-us', { month: 'short', day: 'numeric' });
}

export const getBestChartTypeForAssertion = (
    assertionInfo?: AssertionInfo | Maybe<AssertionInfo>,
): AssertionChartType => {
    switch (assertionInfo?.type) {
        case AssertionType.Freshness:
            return AssertionChartType.Freshness;
        case AssertionType.Field:
            return AssertionChartType.ValuesOverTime;
        case AssertionType.Sql:
            return AssertionChartType.ValuesOverTime;
        case AssertionType.Volume:
            return AssertionChartType.ValuesOverTime;
        default:
            break;
    }
    return AssertionChartType.StatusOverTime; // safest catch-all fallback
};

export const tryGetUpperAndLowerYRangeFromAssertionRunEvent = (runEvent: AssertionRunEvent) => {
    return tryGetExpectedRangeFromAssertionRunEvent(runEvent);
};

const DATA_POINTS_TEMPORAL_ORDER_BY_KEY: keyof AssertionDataPoint = 'time';

const tryGetWindowStartDateForFreshnessAssertionSinceTheLastCheck = (
    mountedDataPoint: AssertionDataPoint,
    allDataPoints: AssertionDataPoint[],
) => {
    // Get the ts of the data point before this one
    const orderedDataPoints = _.sortBy(allDataPoints, DATA_POINTS_TEMPORAL_ORDER_BY_KEY);
    const thisDataPointIndex = orderedDataPoints.findIndex((point) => point.time === mountedDataPoint.time);
    const lastDataPoint: AssertionDataPoint | undefined = orderedDataPoints[thisDataPointIndex - 1];
    return typeof lastDataPoint?.time === 'number' ? new Date(lastDataPoint.time) : undefined;
};

const tryGetWindowStartDateForFreshnessAssertionFixedInterval = (
    mountedDataPoint: AssertionDataPoint,
    assertionInfo: FreshnessAssertionInfo,
): Date | undefined => {
    // Get the current run time minus interval
    const interval = assertionInfo.schedule.fixedInterval;
    if (!interval) return undefined;
    const intervalInMS = INTERVAL_TO_MS[interval.unit] * interval.multiple;
    const windowStartMillis = mountedDataPoint.time - intervalInMS;
    return windowStartMillis > 0 ? new Date(windowStartMillis) : undefined;
};

const tryGetWindowStartDateForFreshnessAssertionCron = (mountedDataPoint: AssertionDataPoint): Date | undefined => {
    // Compute the previous cron iteration of this data point
    const cron = mountedDataPoint.relatedRunEvent.result?.assertion?.freshnessAssertion?.schedule?.cron;
    if (!cron) return undefined;
    let lastExpectedRunTS = getPreviousScheduleEvaluationTimeMs(
        {
            cron: cron.cron,
            timezone: cron.timezone,
        },
        mountedDataPoint.time,
    );

    if (typeof lastExpectedRunTS !== 'number') {
        return undefined;
    }

    // If lastExpectedRunTS is actually the scheduled run time of this data point, correct it
    // NOTE: in some cases a slight delay in running the assertion can cause mountedDataPoint.time to be...
    // ...slightly after the cron run time. So, we need to take one more step back on the cron schedule...
    // ...in order to get the actual time of the last window
    const oneSchedulePriorTS =
        getPreviousScheduleEvaluationTimeMs(
            {
                cron: cron.cron,
                timezone: cron.timezone,
            },
            lastExpectedRunTS,
        ) || 0;

    // ie. let's say the assertion ran 3min after it was scheduled to run, then below value would be 3min as MS
    const distanceOfDataPointFromLastScheduledTime = mountedDataPoint.time - lastExpectedRunTS;
    // ie. let's say the cron schedules this to run every 24h, then below value would be 24h as MS
    const distanceBetweenScheduledTimes = lastExpectedRunTS - oneSchedulePriorTS;

    // If the distance between runs (ie. 24h) is greater than...
    // ...the distance between this data point and the cron schedule prior to it,
    // ...then we should start the window at the prior schedule (24h3m ago)
    if (distanceOfDataPointFromLastScheduledTime < distanceBetweenScheduledTimes) {
        lastExpectedRunTS = oneSchedulePriorTS;
    }

    return typeof lastExpectedRunTS === 'number' ? new Date(lastExpectedRunTS) : undefined;
};

/**
 * Gets start and end dates of a freshness asseriton's evaluation window
 * Ie. if it's a fixed interval then it'll give the {current run's date - interval} and {current run date}
 * Ie. if it's a cron then it'll give the dates of the last run
 * @param mountedDataPoint
 * @param allDataPoints
 * @returns {tuple:[Date, Date]} start and end date
 */
export const getWindowStartAndEndDatesForFreshnessAssertionRun = (
    mountedDataPoint: AssertionDataPoint | undefined,
    allDataPoints: AssertionDataPoint[],
): [Date, Date] | undefined => {
    // 1. ensure data point is valid
    const assertionInfo = mountedDataPoint?.relatedRunEvent?.result?.assertion;
    if (
        !mountedDataPoint?.time ||
        assertionInfo?.type !== AssertionType.Freshness ||
        !assertionInfo.freshnessAssertion
    ) {
        return undefined;
    }
    if (mountedDataPoint.result.type === AssertionResultType.Error) {
        return undefined;
    }
    // 2. Get the end of the window
    // NOTE: assertion run time is the end of the window
    const windowEndDate = new Date(mountedDataPoint.time);

    // 3. If window details exist in native results, use those
    const nativeResults = mountedDataPoint.relatedRunEvent.result?.nativeResults;
    if (nativeResults) {
        let maybeStartTime =
            nativeResults[
                ASSERTION_NATIVE_RESULTS_KEYS_BY_ASSERTION_TYPE.FRESHNESS_ASSERTIONS.EVALUATION_WINDOW_START_TIME
            ];
        if (typeof maybeStartTime === 'string') {
            maybeStartTime = parseInt(maybeStartTime, 10);
        }
        if (typeof maybeStartTime === 'number' && !Number.isNaN(maybeStartTime)) {
            return [new Date(maybeStartTime), windowEndDate];
        }
    }

    // 4. Get the start of the window
    let windowStartDate: Date | undefined;
    switch (assertionInfo.freshnessAssertion.schedule.type) {
        case FreshnessAssertionScheduleType.SinceTheLastCheck: {
            windowStartDate = tryGetWindowStartDateForFreshnessAssertionSinceTheLastCheck(
                mountedDataPoint,
                allDataPoints,
            );
            break;
        }
        case FreshnessAssertionScheduleType.FixedInterval: {
            windowStartDate = tryGetWindowStartDateForFreshnessAssertionFixedInterval(
                mountedDataPoint,
                assertionInfo.freshnessAssertion,
            );
            break;
        }
        case FreshnessAssertionScheduleType.Cron: {
            windowStartDate = tryGetWindowStartDateForFreshnessAssertionCron(mountedDataPoint);
            break;
        }
        default:
            break;
    }
    if (!windowStartDate) {
        return undefined;
    }

    // 5. Return the values
    return [windowStartDate, windowEndDate];
};

/**
 * Duplicates a datapoint with +- a certain amount of buffer on the timestamp
 * Useful to extend a line with some buffer to the left and right
 * @param dataPoint
 * @param timestampMillisModifiers
 * @returns {AssertionDataPoint[]}
 */
export const duplicateDataPointsAcrossBufferedTimeRange = (
    dataPoint: AssertionDataPoint,
    timestampMillisBuffer: number,
): AssertionDataPoint[] => {
    const timestampMillisModifiers = [-timestampMillisBuffer, 0, timestampMillisBuffer];
    const points: AssertionDataPoint[] = timestampMillisModifiers.map((tsModifier) => ({
        ...dataPoint,
        time: dataPoint.time + tsModifier,
    }));
    return points;
};

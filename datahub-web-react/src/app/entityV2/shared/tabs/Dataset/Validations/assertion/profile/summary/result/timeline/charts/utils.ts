import _ from 'lodash';

import {
    AssertionInfo,
    AssertionResultType,
    AssertionType,
    Maybe,
} from '../../../../../../../../../../../../types.generated';
import { AssertionChartType, AssertionDataPoint } from './types';

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

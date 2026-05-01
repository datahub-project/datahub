import _ from 'lodash';
import { DefaultTheme } from 'styled-components';

import { AssertionResultType } from '@types';

export const getFillColor = (type: AssertionResultType, theme: DefaultTheme) => {
    const SUCCESS_COLOR_HEX = theme.colors.iconSuccess;
    const FAILURE_COLOR_HEX = theme.colors.iconError;
    const ERROR_COLOR_HEX = theme.colors.iconWarning;
    const INIT_COLOR_HEX = theme.colors.textTertiary;
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

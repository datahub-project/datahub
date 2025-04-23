import _ from 'lodash';

import { MAX_NUM_LATEST_UPDATES_SHOWN } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/freshness/DatasetFreshnessScheduleBuilder';
import {
    ONE_DAY_IN_MS,
    ONE_HOUR_IN_MS,
    ONE_WEEK_IN_MS,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/result/timeline/utils';
import { NUMBER_DISPLAY_PRECISION } from '@app/entityV2/shared/tabs/Dataset/Validations/shared/constant';

import { Operation } from '@types';

/**
 * Compute the average length of time between update operations in milliseconds.
 *
 * Example usage:
 *
 * const operations = [
 *     { ..., lastUpdatedTimestamp: 1672531200000 }, // Jan 1, 2025
 *     { ..., lastUpdatedTimestamp: 1672617600000 }, // Jan 2, 2025
 *     { ..., lastUpdatedTimestamp: 1672704000000 }, // Jan 3, 2025
 * ];
 * const averageTime = computeAverageUpdateFrequencyInMillis(operations);
 * console.log(`Average time between updates: ${averageTime} ms`); // Output: Average time between updates: 86400000 ms (1 day)
 *
 * @param operations The list of operations to process. Uses the lastUpdatedTimestamp (the actual time the update was performed).
 * @returns The average time between updates in milliseconds.
 */
export const computeAverageUpdateFrequencyInMillis = (operations: Operation[]) => {
    if (!operations || operations.length <= 1) {
        return NaN;
    }

    // This assumes operations are already sorted by lastUpdatedTimestamp in descending order
    const earliestOperationTimestamp = operations[operations.length - 1].lastUpdatedTimestamp;
    const latestOperationTimestamp = operations[0].lastUpdatedTimestamp;

    const totalTime = latestOperationTimestamp - earliestOperationTimestamp;

    const averageTime = totalTime / operations.length;
    return averageTime;
};

/**
 * Compute the time since the most recent operations in milliseconds.
 *
 * Example usage:
 *
 * const operations = [
 *     { ..., lastUpdatedTimestamp: 1672531200000 }, // Jan 1, 2025
 *     { ..., lastUpdatedTimestamp: 1672617600000 }, // Jan 2, 2025
 *     { ..., lastUpdatedTimestamp: 1672704000000 }, // Jan 3, 2025
 * ];
 * const result = mostRecentOperationsTimeSinceInMillis(operations, 2);  // Note only 2 operations are considered.
 * console.log(result);
 * // Output: [
 * //   { delta: <time_since_Jan_3_2025_in_ms>, lastUpdatedTimestamp: 1672704000000 },
 * //   { delta: <time_since_Jan_2_2025_in_ms>, lastUpdatedTimestamp: 1672617600000 }
 * // ]
 *
 * @param operations The list of operations to process.
 * @param maxNumOperations The maximum number of operations to consider.
 * @returns An array of objects containing the time since the last updated timestamp { delta: number; lastUpdatedTimestamp: number; }
 *  Where `delta` is the time from now since the update in ms and `lastUpdatedTimestamp` is the timestamp of the actual update in ms.
 */
export const mostRecentOperationsTimeSinceInMillis = (
    operations: Operation[],
    maxNumOperations = MAX_NUM_LATEST_UPDATES_SHOWN,
): { key: string; delta: number; lastUpdatedTimestamp: number }[] => {
    // This assumes operations are already sorted by lastUpdatedTimestamp in descending order
    if (!operations || operations.length === 0) {
        return [];
    }
    const nowMs = Date.now();

    return operations.slice(0, maxNumOperations).map((operation) => ({
        key: `${operation.lastUpdatedTimestamp}-${operation.operationType}`,
        delta: nowMs - operation.lastUpdatedTimestamp,
        lastUpdatedTimestamp: operation.lastUpdatedTimestamp,
    }));
};

export interface TimeDeltaStrings {
    seconds: string;
    minutes: string;
    hours: string;
    days: string;
    weeks: string;
    years: string;
}

const SEC_IN_MS = 1000;
const MIN_IN_MS = 60 * SEC_IN_MS;
const YEAR_IN_MS = 365 * ONE_DAY_IN_MS;

/**
 * Round to decimal digit precision set by NUMBER_DISPLAY_PRECISION.
 * @param num number to round
 * @returns number rounded to NUMBER_DISPLAY_PRECISION decimal place maximum.
 */
const round = (num: number): number => {
    return _.round(num, NUMBER_DISPLAY_PRECISION);
};

/**
 * Convert the difference between two timestamps (in milliseconds) into a human readable formatted string using the largest non-zero unit.
 *
 * Example usage:
 *
 * const earlierTimestamp = 1672531200000; // Jan 1, 2025
 * const laterTimestamp = 1672704000000; // Jan 3, 2025
 * const result = getFormattedTimeStringTimeSince(earlierTimestamp, laterTimestamp);
 * console.log(result); // Output: "2 days ago"
 *
 * const earlierTimestampYesterday = 1672617600000; // Jan 2, 2025
 * const laterTimestampYesterday = 1672704000000; // Jan 3, 2025
 * const resultYesterday = getFormattedTimeStringTimeSince(earlierTimestampYesterday, laterTimestampYesterday);
 * console.log(resultYesterday); // Output: "yesterday"
 *
 * const earlierTimestamp2_5DaysAgo = 1672444800000; // Dec 31, 2024 12:00 PM
 * const laterTimestamp2_5DaysAgo = 1672704000000; // Jan 3, 2025
 * const result2_5DaysAgo = getFormattedTimeStringTimeSince(earlierTimestamp2_5DaysAgo, laterTimestamp2_5DaysAgo);
 * console.log(result2_5DaysAgo); // Output: "2.5 days ago"
 *
 * @param earlierTimestamp earlier timestamp for comparison.
 * @param laterTimestamp later timestamp for comparison.
 * @param roundFn function to round the numeric output.
 * @returns string representing the time difference.
 */
export const getFormattedTimeStringTimeSince = (
    earlierTimestamp: number,
    laterTimestamp: number,
    roundFn: (num: number) => number = round,
): string => {
    // If the time is less than 1 day ago, show the time of day in hours and minutes.
    const timeDelta = laterTimestamp - earlierTimestamp;

    const timeStrings: TimeDeltaStrings = {
        seconds: `${roundFn(timeDelta / SEC_IN_MS)} second${timeDelta / SEC_IN_MS === 1 ? '' : 's'}`,
        minutes: `${roundFn(timeDelta / MIN_IN_MS)} minute${timeDelta / MIN_IN_MS === 1 ? '' : 's'}`,
        hours: `${roundFn(timeDelta / ONE_HOUR_IN_MS)} hour${timeDelta / ONE_HOUR_IN_MS === 1 ? '' : 's'}`,
        days: `${roundFn(timeDelta / ONE_DAY_IN_MS)} day${timeDelta / ONE_DAY_IN_MS === 1 ? '' : 's'}`,
        weeks: `${roundFn(timeDelta / ONE_WEEK_IN_MS)} week${timeDelta / ONE_WEEK_IN_MS === 1 ? '' : 's'}`,
        years: `${roundFn(timeDelta / YEAR_IN_MS)} year${timeDelta / YEAR_IN_MS === 1 ? '' : 's'}`,
    };

    if (timeDelta < SEC_IN_MS) {
        return 'now';
    }
    if (timeDelta < MIN_IN_MS) {
        return `${timeStrings.seconds} ago`;
    }
    if (timeDelta < ONE_HOUR_IN_MS) {
        return `${timeStrings.minutes} ago`;
    }
    if (timeDelta < ONE_DAY_IN_MS) {
        return `${timeStrings.hours} ago`;
    }
    if (timeDelta < ONE_WEEK_IN_MS) {
        if (timeDelta / ONE_DAY_IN_MS >= 1 && timeDelta / ONE_DAY_IN_MS < 2) {
            return 'yesterday';
        }
        return `${timeStrings.days} ago`;
    }
    if (timeDelta < YEAR_IN_MS) {
        return `${timeStrings.weeks} ago`;
    }
    if (timeDelta >= YEAR_IN_MS) {
        return `${timeStrings.years} ago`;
    }
    return '';
};

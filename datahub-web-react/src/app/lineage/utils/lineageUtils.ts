import dayjs from 'dayjs';

const MILLIS_PER_HOUR = 3600000;

/**
 * Returns the default time-lineage start time which is 28 days - current time, rounded down to the nearest hour.
 */
export const getDefaultLineageStartTime = () => {
    return Math.floor(dayjs().subtract(28, 'day').valueOf() / MILLIS_PER_HOUR) * MILLIS_PER_HOUR;
};

/**
 * Returns the default time-lineage start time which is the current time round up to the nearest hour.
 */
export const getDefaultLineageEndTime = () => {
    return Math.ceil(dayjs().valueOf() / MILLIS_PER_HOUR) * MILLIS_PER_HOUR;
};

/**
 * Returns the default time-lineage header text.
 */
export const getDefaultLineageHeaderText = () => {
    return 'Last 28 days';
};

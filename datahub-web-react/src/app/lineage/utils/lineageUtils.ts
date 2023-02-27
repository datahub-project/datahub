import dayjs from 'dayjs';

const MILLIS_PER_HOUR = 3600000;

/**
 * Returns the default time-lineage start time which is 14 days - current time, rounded down to the nearest hour.
 */
export const getDefaultLineageStartTime = () => {
    return Math.floor(dayjs().subtract(14, 'day').valueOf() / MILLIS_PER_HOUR) * MILLIS_PER_HOUR;
};

/**
 * Returns the default time-lineage start time which is the current time round up to the nearest hour.
 */
export const getDefaultLineageEndTime = () => {
    return Math.ceil(dayjs().valueOf() / MILLIS_PER_HOUR) * MILLIS_PER_HOUR;
};

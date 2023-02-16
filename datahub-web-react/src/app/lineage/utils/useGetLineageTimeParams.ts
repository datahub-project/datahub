import * as QueryString from 'query-string';
import { useLocation } from 'react-router-dom';
import { getDefaultLineageEndTime, getDefaultLineageStartTime } from './lineageUtils';

export const START_TIME_MILLIS_URL_PARAM = 'start_time_millis';
export const END_TIME_MILLIS_URL_PARAM = 'end_time_millis';

export function useGetLineageTimeParams() {
    const location = useLocation();
    const params = QueryString.parse(location.search, { arrayFormat: 'comma' });
    const startTimeMillisString = params[START_TIME_MILLIS_URL_PARAM] as string;
    const endTimeMillisString = params[END_TIME_MILLIS_URL_PARAM] as string;

    let startTimeMillis = startTimeMillisString ? parseInt(startTimeMillisString, 10) : null;
    let endTimeMillis = endTimeMillisString ? parseInt(endTimeMillisString, 10) : null;

    // Establish default parameters -> last 14 days.
    if (startTimeMillis === null || endTimeMillis === null) {
        startTimeMillis = getDefaultLineageStartTime();
        endTimeMillis = getDefaultLineageEndTime();
    }

    return { startTimeMillis, endTimeMillis };
}

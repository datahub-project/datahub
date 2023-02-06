import * as QueryString from 'query-string';
import { useLocation } from 'react-router-dom';

export const START_TIME_MILLIS_URL_PARAM = 'start_time_millis';
export const END_TIME_MILLIS_URL_PARAM = 'end_time_millis';

export function useGetTimeParams() {
    const location = useLocation();
    const params = QueryString.parse(location.search, { arrayFormat: 'comma' });
    const startTimeMillisString = params[START_TIME_MILLIS_URL_PARAM] as string;
    const endTimeMillisString = params[END_TIME_MILLIS_URL_PARAM] as string;
    const startTimeMillis = startTimeMillisString ? parseInt(startTimeMillisString, 10) : null;
    const endTimeMillis = endTimeMillisString ? parseInt(endTimeMillisString, 10) : null;
    return { startTimeMillis, endTimeMillis };
}

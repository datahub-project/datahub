import * as QueryString from 'query-string';
import { useLocation } from 'react-router-dom';
import moment from 'moment';
import { useMemo } from 'react';
import { useAppConfig } from '../../useAppConfig';

export const START_TIME_MILLIS_URL_PARAM = 'start_time_millis';
export const END_TIME_MILLIS_URL_PARAM = 'end_time_millis';
export const SHOW_ALL_TIME_LINEAGE_URL_PARAM = 'show_all_time_lineage';

export function useGetDefaultLineageStartTimeMillis() {
    const appConfig = useAppConfig();
    const { defaultLastDaysFilter } = appConfig.config.lineageConfig;
    return useMemo(
        () =>
            defaultLastDaysFilter
                ? moment()
                      .subtract(defaultLastDaysFilter, 'days')
                      .set({ hour: 0, minute: 0, second: 0, millisecond: 0 })
                      .valueOf()
                : null,
        [defaultLastDaysFilter],
    );
}

export function useGetLineageTimeParams() {
    const startTimeMillisDefault = useGetDefaultLineageStartTimeMillis();

    const location = useLocation();
    const params = QueryString.parse(location.search, { arrayFormat: 'comma' });
    const startTimeMillisString = params[START_TIME_MILLIS_URL_PARAM] as string;
    const endTimeMillisString = params[END_TIME_MILLIS_URL_PARAM] as string;
    const showAllTimeLineage = (params[SHOW_ALL_TIME_LINEAGE_URL_PARAM] as string) === 'true';

    let startTimeMillis: number | undefined = startTimeMillisString ? parseInt(startTimeMillisString, 10) : undefined;
    const endTimeMillis: number | undefined = endTimeMillisString ? parseInt(endTimeMillisString, 10) : undefined;

    // if show all time or start/end times are not explicitly set, use defaults from instance configuration
    if (!showAllTimeLineage && !startTimeMillis && !endTimeMillis && startTimeMillisDefault) {
        startTimeMillis = startTimeMillisDefault;
    }

    const isDefault = showAllTimeLineage
        ? !startTimeMillisDefault
        : (startTimeMillis ?? null) === (startTimeMillisDefault ?? null) && !endTimeMillis;
    return { startTimeMillis, endTimeMillis, isDefault };
}

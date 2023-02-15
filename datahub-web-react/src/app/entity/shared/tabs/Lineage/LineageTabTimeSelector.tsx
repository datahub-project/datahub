import React from 'react';
import moment from 'moment';
import { useHistory, useLocation } from 'react-router';
import dayjs from 'dayjs';
import analytics, { EventType } from '../../../../analytics';
import LineageTimeSelector from '../../../../lineage/LineageTimeSelector';
import { getTimeFromNow } from '../../../../shared/time/timeUtils';
import updateQueryParams from '../../../../shared/updateQueryParams';
import { useGetTimeParams } from '../../../../lineage/utils/useGetTimeParams';

export default function LineageTabTimeSelector() {
    const history = useHistory();
    const location = useLocation();
    const { startTimeMillis, endTimeMillis } = useGetTimeParams();

    const lineageTimeSelectorOnChange = (dates, _dateStrings) => {
        if (dates) {
            const [start, end] = dates;
            const startTimeMillisValue = start?.valueOf() || undefined;
            const endTimeMillisValue = end?.valueOf() || undefined;
            analytics.event({
                type: EventType.LineageGraphTimeRangeSelectionEvent,
                relativeStartDate: getTimeFromNow(startTimeMillisValue),
                relativeEndDate: getTimeFromNow(endTimeMillisValue),
            });

            updateQueryParams(
                { start_time_millis: startTimeMillisValue, end_time_millis: endTimeMillisValue },
                location,
                history,
            );
        }
    };

    return (
        <LineageTimeSelector
            onChange={lineageTimeSelectorOnChange}
            initialDates={[
                moment(startTimeMillis || dayjs().subtract(14, 'day').valueOf()),
                moment(endTimeMillis || dayjs().valueOf()),
            ]}
        />
    );
}

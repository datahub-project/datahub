import React from 'react';
import moment from 'moment';
import { useHistory, useLocation } from 'react-router';
import analytics, { EventType } from '../../../../analytics';
import LineageTimeSelector from '../../../../lineage/LineageTimeSelector';
import { getTimeFromNow } from '../../../../shared/time/timeUtils';
import updateQueryParams from '../../../../shared/updateQueryParams';
import { useGetLineageTimeParams } from '../../../../lineage/utils/useGetLineageTimeParams';

export default function LineageTabTimeSelector() {
    const history = useHistory();
    const location = useLocation();
    const { startTimeMillis, endTimeMillis } = useGetLineageTimeParams();

    const lineageTimeSelectorOnChange = (dates, _dateStrings) => {
        if (dates) {
            const [start, end] = dates;
            const startTimeMillisValue = start?.valueOf();
            const endTimeMillisValue = end?.valueOf();
            const relativeStartDate = getTimeFromNow(startTimeMillisValue);
            const relativeEndDate = getTimeFromNow(endTimeMillisValue);
            analytics.event({
                type: EventType.LineageGraphTimeRangeSelectionEvent,
                relativeStartDate,
                relativeEndDate,
            });

            updateQueryParams(
                { start_time_millis: startTimeMillisValue, end_time_millis: endTimeMillisValue },
                location,
                history,
            );
        }
    };

    const initialDates: [moment.Moment | null, moment.Moment | null] = [
        startTimeMillis ? moment(startTimeMillis) : null,
        endTimeMillis ? moment(endTimeMillis) : null,
    ];

    return <LineageTimeSelector onChange={lineageTimeSelectorOnChange} initialDates={initialDates} />;
}

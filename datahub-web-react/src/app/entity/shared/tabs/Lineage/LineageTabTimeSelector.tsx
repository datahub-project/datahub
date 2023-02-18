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
                (startTimeMillis && startTimeMillis > 0 && moment(startTimeMillis)) || null,
                moment(endTimeMillis),
            ]}
        />
    );
}

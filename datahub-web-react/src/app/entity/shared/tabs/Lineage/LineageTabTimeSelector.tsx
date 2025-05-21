import moment from 'moment';
import React from 'react';
import { useHistory, useLocation } from 'react-router';

import analytics, { EventType } from '@app/analytics';
import LineageTimeSelector from '@app/lineage/LineageTimeSelector';
import { useGetLineageTimeParams } from '@app/lineage/utils/useGetLineageTimeParams';
import { getTimeFromNow } from '@app/shared/time/timeUtils';
import updateQueryParams from '@app/shared/updateQueryParams';

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

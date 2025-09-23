import React, { useCallback } from 'react';
import { useHistory, useLocation } from 'react-router';

import analytics, { EventType } from '@app/analytics';
import { useGetLineageTimeParams } from '@app/lineage/utils/useGetLineageTimeParams';
import LineageTimeSelector, { Datetime } from '@app/lineageV2/LineageTimeSelector';
import { getTimeFromNow } from '@app/shared/time/timeUtils';
import updateQueryParams from '@app/shared/updateQueryParams';

export default function LineageTabTimeSelector() {
    const history = useHistory();
    const location = useLocation();
    const { startTimeMillis, endTimeMillis } = useGetLineageTimeParams();

    const lineageTimeSelectorOnChange = useCallback(
        (start: Datetime, end: Datetime) => {
            const startTimeMillisValue = start?.valueOf();
            const endTimeMillisValue = end?.valueOf();
            const relativeStartDate = getTimeFromNow(startTimeMillisValue);
            const relativeEndDate = getTimeFromNow(endTimeMillisValue);
            analytics.event({
                type: EventType.LineageGraphTimeRangeSelectionEvent,
                relativeStartDate,
                relativeEndDate,
            });

            const isAllTimeLineage = !start && !end;

            updateQueryParams(
                {
                    start_time_millis: startTimeMillisValue?.toString(),
                    end_time_millis: endTimeMillisValue?.toString(),
                    show_all_time_lineage: isAllTimeLineage ? 'true' : undefined,
                },
                location,
                history,
            );
        },
        [history, location],
    );

    return (
        <LineageTimeSelector
            onChange={lineageTimeSelectorOnChange}
            startTimeMillis={startTimeMillis}
            endTimeMillis={endTimeMillis}
        />
    );
}

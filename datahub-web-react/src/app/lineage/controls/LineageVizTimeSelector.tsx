import React from 'react';
import moment from 'moment';
import { useHistory, useLocation } from 'react-router';

import { navigateToLineageUrl } from '../utils/navigateToLineageUrl';
import analytics, { EventType } from '../../analytics';
import { getTimeFromNow } from '../../shared/time/timeUtils';
import LineageTimeSelector from '../LineageTimeSelector';
import { useGetTimeParams } from '../utils/useGetTimeParams';

type Props = {
    isHideSiblingMode: boolean;
    showColumns: boolean;
};

export default function LineageVizTimeSelector({ isHideSiblingMode, showColumns }: Props) {
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

            navigateToLineageUrl({
                location,
                history,
                isLineageMode: true,
                isHideSiblingMode,
                showColumns,
                startTimeMillis: startTimeMillisValue,
                endTimeMillis: endTimeMillisValue,
            });
        }
    };

    return (
        <LineageTimeSelector
            onChange={lineageTimeSelectorOnChange}
            initialDates={[
                startTimeMillis ? moment(startTimeMillis) : null,
                endTimeMillis ? moment(endTimeMillis) : null,
            ]}
        />
    );
}

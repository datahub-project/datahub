import React from 'react';
import moment from 'moment';
import { useHistory, useLocation } from 'react-router';
import { navigateToLineageUrl } from '../utils/navigateToLineageUrl';
import analytics, { EventType } from '../../analytics';
import { getTimeFromNow } from '../../shared/time/timeUtils';
import LineageTimeSelector from '../LineageTimeSelector';
import { useGetLineageTimeParams } from '../utils/useGetLineageTimeParams';
import { getDefaultLineageStartTime, getDefaultLineageEndTime } from '../utils/lineageUtils';

type Props = {
    isHideSiblingMode: boolean;
    showColumns: boolean;
};

export default function LineageVizTimeSelector({ isHideSiblingMode, showColumns }: Props) {
    const history = useHistory();
    const location = useLocation();
    const { startTimeMillis, endTimeMillis } = useGetLineageTimeParams();
    const finalStartTimeMillis = startTimeMillis === undefined ? getDefaultLineageStartTime() : startTimeMillis;
    const finalEndTimeMillis = endTimeMillis === undefined ? getDefaultLineageEndTime() : endTimeMillis;

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
                (finalStartTimeMillis && finalStartTimeMillis > 0 && moment(finalStartTimeMillis)) || null,
                moment(finalEndTimeMillis),
            ]}
        />
    );
}

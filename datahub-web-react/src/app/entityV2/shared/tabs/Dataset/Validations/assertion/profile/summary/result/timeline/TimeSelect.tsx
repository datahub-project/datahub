import { Typography } from 'antd';
import dayjs from 'dayjs';
import React, { useState } from 'react';
import styled from 'styled-components';

import { LOOKBACK_WINDOWS } from '@app/entityV2/shared/tabs/Dataset/Stats/lookbackWindows';
import { CustomTimeRangeSelectDropdown } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/result/timeline/CustomTimeRangeSelectDropdown';
import { SelectablePill } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/shared/SelectablePill';
import { TimeWindow, getFixedLookbackWindow } from '@src/app/shared/time/timeUtils';
import { DateInterval } from '@src/types.generated';

const TimeWindowPills = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
    justify-content: start;
    margin: 24px 4px;
    margin-top: 0px;
`;

const CustomTimeRangeLabel = styled(Typography.Text)`
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    max-width: 120px;
    color: inherit;
`;

const LOOKBACK_WINDOWS_FOR_ASSERTION_VIZ = Object.values(LOOKBACK_WINDOWS).filter(
    (window) => window.windowSize.interval !== DateInterval.Year,
);

const CUSTOM_TIME_RANGE_WINDOW = {
    windowName: 'Custom',
};

export type TimeWindowSelectionItem = {
    window: TimeWindow;
    windowName: string;
};

type Props = {
    timeWindow: TimeWindowSelectionItem;
    setTimeWindow: (newWindow: TimeWindowSelectionItem) => void;
};

export const TimeSelect = ({ timeWindow, setTimeWindow }: Props) => {
    const [isCustomTimeRangeDropdownOpen, setIsCustomTimeRangeDropdownOpen] = useState(false);

    /**
     * Invoked when user selects new lookback window (e.g. 1 month)
     */
    const onChangeLookbackWindow = (window: TimeWindowSelectionItem) => {
        setTimeWindow(window);
    };

    const isCustomRangeSelected = timeWindow.windowName === CUSTOM_TIME_RANGE_WINDOW.windowName;
    const customTimeRangeLabel = isCustomRangeSelected ? (
        <>
            {dayjs(timeWindow.window.startTime).format('MMM D, h:mm A')} -{' '}
            {dayjs(timeWindow.window.endTime).format('MMM D, h:mm A')}
        </>
    ) : (
        CUSTOM_TIME_RANGE_WINDOW.windowName
    );
    return (
        <TimeWindowPills>
            {/* Fixed lookback windows */}
            {Object.values(LOOKBACK_WINDOWS_FOR_ASSERTION_VIZ).map((window) => (
                <SelectablePill
                    text={window.text}
                    selected={timeWindow.windowName === window.text}
                    onSelect={() =>
                        onChangeLookbackWindow({
                            window: getFixedLookbackWindow(window.windowSize),
                            windowName: window.text,
                        })
                    }
                />
            ))}

            {/* Custom time range dropdown */}
            <CustomTimeRangeSelectDropdown
                open={isCustomTimeRangeDropdownOpen}
                onOpenChange={setIsCustomTimeRangeDropdownOpen}
                onChange={(range) =>
                    onChangeLookbackWindow({
                        window: range,
                        windowName: CUSTOM_TIME_RANGE_WINDOW.windowName,
                    })
                }
            >
                <SelectablePill
                    text={<CustomTimeRangeLabel>{customTimeRangeLabel}</CustomTimeRangeLabel>}
                    selected={isCustomRangeSelected}
                    onSelect={() => setIsCustomTimeRangeDropdownOpen((isOpen) => !isOpen)}
                />
            </CustomTimeRangeSelectDropdown>
        </TimeWindowPills>
    );
};

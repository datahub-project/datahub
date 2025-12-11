/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

import { TickLabel } from '@components/components/CalendarChart/private/components/TickLabel';
import { DAYS_IN_WEEK } from '@components/components/CalendarChart/private/constants';
import { useCalendarState } from '@components/components/CalendarChart/private/context';
import { AxisBottomMonthsProps } from '@components/components/CalendarChart/types';

export function AxisBottomMonths<ValueType>({ labelProps }: AxisBottomMonthsProps) {
    const { squareSize, squareGap, margin, data } = useCalendarState<ValueType>();
    const weeksInMonth = data.map((group) => group.weeks.length);
    const axisTopMargin = 25;

    return (
        <>
            {data.map((month, monthIndex) => {
                // Do not show the first label when there are only one week in month to prevent labels overlay
                if (monthIndex === 0 && month.weeks.length === 1) return null;

                const weeksBefore = weeksInMonth.slice(0, monthIndex).reduce((acc, value) => acc + value, 0);
                const yLabel = DAYS_IN_WEEK * (squareSize + squareGap) + margin.top + axisTopMargin;
                const xLabel = weeksBefore * (squareSize + squareGap) + squareGap * monthIndex + margin.left;

                return (
                    <TickLabel
                        key={`axis-bottom-${month.key}`}
                        text={month.label}
                        x={xLabel}
                        y={yLabel}
                        {...labelProps}
                    />
                );
            })}
        </>
    );
}

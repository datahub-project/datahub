/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React, { useMemo } from 'react';

import { Week } from '@components/components/CalendarChart/private/components/Week';
import { useCalendarState } from '@components/components/CalendarChart/private/context';
import { MonthProps } from '@components/components/CalendarChart/types';

export function Month<ValueType>({ month, monthIndex }: MonthProps<ValueType>) {
    const { squareGap, data } = useCalendarState();
    const monthOffset = useMemo(() => squareGap * monthIndex, [squareGap, monthIndex]);
    const countOfWeeksBefore = useMemo(
        () => data.slice(0, monthIndex).reduce((countOfWeeks, monthItem) => countOfWeeks + monthItem.weeks.length, 0),
        [data, monthIndex],
    );

    return (
        <>
            {month.weeks.map((week, weekIndex) => {
                return (
                    <Week
                        key={week.key}
                        week={week}
                        monthOffset={monthOffset}
                        weekNumber={countOfWeeksBefore + weekIndex}
                    />
                );
            })}
        </>
    );
}

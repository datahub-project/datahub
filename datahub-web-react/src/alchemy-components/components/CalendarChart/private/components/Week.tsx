import React, { useMemo } from 'react';

import { Day } from '@components/components/CalendarChart/private/components/Day';
import { useCalendarState } from '@components/components/CalendarChart/private/context';
import { WeekProps } from '@components/components/CalendarChart/types';

export function Week<ValueType>({ week, weekNumber, monthOffset }: WeekProps<ValueType>) {
    const { squareSize, squareGap, margin } = useCalendarState<ValueType>();

    const x = useMemo(() => {
        const weekOffset = weekNumber * (squareGap + squareSize);
        return monthOffset + weekOffset + margin.left;
    }, [squareGap, squareSize, monthOffset, weekNumber, margin.left]);

    return (
        <>
            {week.days.map((day, dayIndex) => {
                return <Day<ValueType> key={day.key} day={day} weekOffset={x} dayIndex={dayIndex} />;
            })}
        </>
    );
}

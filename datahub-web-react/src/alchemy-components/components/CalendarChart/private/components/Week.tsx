import React, { useMemo } from 'react';
import { WeekProps } from '../../types';
import { useCalendarState } from '../context';
import { Day } from './Day';

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

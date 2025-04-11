import React, { useMemo } from 'react';
import { MonthProps } from '../../types';
import { useCalendarState } from '../context';
import { Week } from './Week';

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

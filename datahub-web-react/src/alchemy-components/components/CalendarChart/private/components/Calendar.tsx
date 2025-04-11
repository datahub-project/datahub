import React from 'react';
import { CalendarProps } from '../../types';
import { Month } from './Month';

export function Calendar<ValueType>({ data }: CalendarProps<ValueType>) {
    return (
        <>
            {data.map((month, monthIndex) => {
                return <Month key={month.key} month={month} monthIndex={monthIndex} />;
            })}
        </>
    );
}

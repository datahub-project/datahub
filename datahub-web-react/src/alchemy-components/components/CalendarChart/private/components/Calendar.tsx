import React from 'react';

import { Month } from '@components/components/CalendarChart/private/components/Month';
import { CalendarProps } from '@components/components/CalendarChart/types';

export function Calendar<ValueType>({ data }: CalendarProps<ValueType>) {
    return (
        <>
            {data.map((month, monthIndex) => {
                return <Month key={month.key} month={month} monthIndex={monthIndex} />;
            })}
        </>
    );
}

import React from 'react';

import { Month } from '@components/components/CalendarChart/private/components/Month';
import { CalendarProps } from '@components/components/CalendarChart/types';

export function Calendar<ValueType>({ data }: CalendarProps<ValueType>) {
    return (
        <>
            {data.map((month, monthIndex) => {
                const uniqueKey = month.weeks[0]?.key ? `${month.key}-${month.weeks[0].key}` : month.key;
                return <Month key={uniqueKey} month={month} monthIndex={monthIndex} />;
            })}
        </>
    );
}

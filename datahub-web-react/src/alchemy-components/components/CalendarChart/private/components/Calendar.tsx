/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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

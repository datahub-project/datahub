/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { DatePicker, DatePickerValue, DatePickerVariant } from '@components';
import React, { useEffect, useState } from 'react';

import {
    dateStringToMoment,
    momentToDateString,
} from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/components/ChangeHistoryDrawer/utils';

type DateSwitcherProps = {
    value?: string | null;
    setValue?: (value: string | null) => void;
};

export default function DateSwitcher({ value, setValue }: DateSwitcherProps) {
    const [internalValue, setInternalValue] = useState<DatePickerValue>(dateStringToMoment(value));

    useEffect(() => setValue?.(momentToDateString(internalValue)), [setValue, internalValue]);

    return (
        <DatePicker
            value={internalValue}
            onChange={(newValue) => setInternalValue(newValue)}
            variant={DatePickerVariant.DateSwitcher}
        />
    );
}

import React from 'react';

import DatePicker from '@utils/DayjsDatePicker';
import dayjs from '@utils/dayjs';
import type { Dayjs } from '@utils/dayjs';

interface Props {
    selectedValues: any[];
    updateSelectedValues: (values: string[] | number[]) => void;
}

export default function DateInput({ selectedValues, updateSelectedValues }: Props) {
    function updateInput(_: Dayjs | null, value: string) {
        updateSelectedValues([value]);
    }

    const currentValue = selectedValues[0] ? dayjs(selectedValues[0]) : undefined;

    return <DatePicker onChange={updateInput} value={currentValue} />;
}

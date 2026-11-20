import React from 'react';

import DatePicker from '@utils/DayjsDatePicker';
import dayjs from '@utils/dayjs';
import type { Dayjs } from '@utils/dayjs';

interface Props {
    selectedValues: any[];
    updateSelectedValues: (values: string[] | number[]) => void;
}

export default function DateInput({ selectedValues, updateSelectedValues }: Props) {
    // antd 5 types dateString as string | string[] to cover multi-select pickers; this one is single
    function updateInput(_: Dayjs | null, value: string | string[]) {
        updateSelectedValues(([] as string[]).concat(value));
    }

    const currentValue = selectedValues[0] ? dayjs(selectedValues[0]) : undefined;

    return <DatePicker onChange={updateInput} value={currentValue} />;
}

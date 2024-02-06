import { DatePicker } from 'antd';
import React from 'react';
import moment, { Moment } from 'moment';

interface Props {
    selectedValues: any[];
    updateSelectedValues: (values: string[] | number[]) => void;
}

export default function DateInput({ selectedValues, updateSelectedValues }: Props) {
    function updateInput(_: Moment | null, value: string) {
        updateSelectedValues([value]);
    }

    const currentValue = selectedValues[0] ? moment(selectedValues[0]) : undefined;

    return <DatePicker onChange={updateInput} value={currentValue} />;
}

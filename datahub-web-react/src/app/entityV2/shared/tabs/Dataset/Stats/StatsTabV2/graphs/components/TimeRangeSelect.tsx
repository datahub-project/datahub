import React from 'react';
import { SelectOption, SimpleSelect } from '@src/alchemy-components';

type TimeRangeSelectProps = {
    options: SelectOption[];
    loading: boolean;
    values: string[];
    onUpdate: (values: string[]) => void;
};

export default function TimeRangeSelect({ options, values, loading, onUpdate }: TimeRangeSelectProps) {
    if (!loading && options.length === 0) return null;

    return (
        <SimpleSelect
            icon="CalendarToday"
            placeholder="Choose time range"
            options={options}
            values={values}
            onUpdate={onUpdate}
            isDisabled={loading}
            showClear={false}
            width="full"
        />
    );
}

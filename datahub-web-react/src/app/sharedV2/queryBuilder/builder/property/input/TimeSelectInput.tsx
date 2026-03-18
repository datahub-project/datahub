import dayjs from '@utils/dayjs';
import React from 'react';

import DatePicker from '@utils/DayjsDatePicker';

type Props = {
    selected?: string[];
    placeholder?: string;
    style?: any;
    onChangeSelected: (newSelectedIds: string[] | undefined) => void;
};

// Simply extracts and returns the first time if it is a time, null otherwise.
export const getValue = (selected) => {
    if (selected && selected?.length) {
        const firstItem: number = +selected[0];
        if (!Number.isNaN(firstItem)) {
            // It's a number!
            return dayjs(firstItem);
        }
    }
    return undefined;
};

export const TimeSelectInput = ({ selected, placeholder, style, onChangeSelected }: Props) => {
    const onSelect = (time) => {
        const timeInMillisSinceEpoch = time.valueOf();
        const newSelected = [timeInMillisSinceEpoch];
        onChangeSelected(newSelected);
    };

    return (
        <DatePicker
            style={style}
            value={getValue(selected)}
            placeholder={placeholder || 'Select time (local)...'}
            onSelect={onSelect}
            showTime
        />
    );
};

import React from 'react';

import DatePicker from '@utils/DayjsDatePicker';
import dayjs from '@utils/dayjs';

const DISPLAY_FORMAT = 'll';

type Props = {
    selected?: string[];
    placeholder?: string;
    onChangeSelected: (newSelected: string[]) => void;
};

// Date structured properties are indexed as epoch millis, so the picked date is stored as a
// millis string (matching the search filter behaviour) and parsed back for display.
export default function TimeSelectValueInput({ selected, placeholder, onChangeSelected }: Props) {
    const storedMillis = selected?.[0];
    const value = storedMillis ? dayjs(Number(storedMillis)) : undefined;

    return (
        <DatePicker
            value={value}
            format={DISPLAY_FORMAT}
            placeholder={placeholder}
            showToday={false}
            onChange={(picked) => onChangeSelected(picked ? [picked.valueOf().toString()] : [])}
        />
    );
}

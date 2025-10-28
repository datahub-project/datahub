import React from 'react';

import { SelectOption, SimpleSelect } from '@src/alchemy-components';
import analytics, { EventType } from '@src/app/analytics';

type TimeRangeSelectProps = {
    options: SelectOption[];
    loading?: boolean;
    values: string[];
    chartName: string;
    onUpdate: (values: string) => void;
};

export default function TimeRangeSelect({ options, values, loading, onUpdate, chartName }: TimeRangeSelectProps) {
    // don't show select if we have only one option or no options at all
    if (!loading && options.length < 2) return null;

    function handleUpdate(newValues: string[]) {
        const lookBackValue = newValues[0];
        analytics.event({ type: EventType.FilterStatsChartLookBack, lookBackValue, chartName });
        onUpdate(lookBackValue);
    }

    return (
        <SimpleSelect
            dataTestId="timerange-select"
            icon="CalendarToday"
            placeholder="Choose time range"
            options={options}
            values={values}
            onUpdate={handleUpdate}
            isDisabled={loading}
            showClear={false}
            width="full"
        />
    );
}

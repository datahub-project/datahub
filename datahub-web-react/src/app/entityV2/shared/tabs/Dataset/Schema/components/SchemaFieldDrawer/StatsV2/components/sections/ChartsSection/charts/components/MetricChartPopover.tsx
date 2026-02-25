import dayjs from 'dayjs';
import React from 'react';

import { Text } from '@src/alchemy-components';
import { Datum } from '@src/alchemy-components/components/LineChart/types';

interface MetricChartPopoverProps {
    datum: Datum;
    renderDatumMetric: (datum: Datum) => React.ReactNode;
}

export default function MetricChartPopover({ datum, renderDatumMetric }: MetricChartPopoverProps) {
    return (
        <>
            <Text size="sm">{dayjs(datum.x).format('dddd, MMM, D ‘YY')}</Text>
            <Text weight="semiBold" size="sm">
                {renderDatumMetric(datum)}
            </Text>
        </>
    );
}

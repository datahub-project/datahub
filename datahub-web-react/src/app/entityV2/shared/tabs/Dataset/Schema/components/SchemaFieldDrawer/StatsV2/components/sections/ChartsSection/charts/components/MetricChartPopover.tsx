import React from 'react';

import { Text } from '@src/alchemy-components';
import { Datum } from '@src/alchemy-components/components/LineChart/types';
import dayjs from '@utils/dayjs';

interface MetricChartPopoverProps {
    datum: Datum;
    renderDatumMetric: (datum: Datum) => React.ReactNode;
}

const DATE_FORMAT = 'dddd, MMM, D ‘YY';

export default function MetricChartPopover({ datum, renderDatumMetric }: MetricChartPopoverProps) {
    return (
        <>
            <Text size="sm" color="gray">
                {dayjs(datum.x).format(DATE_FORMAT)}
            </Text>
            <Text weight="semiBold" size="sm" color="gray">
                {renderDatumMetric(datum)}
            </Text>
        </>
    );
}

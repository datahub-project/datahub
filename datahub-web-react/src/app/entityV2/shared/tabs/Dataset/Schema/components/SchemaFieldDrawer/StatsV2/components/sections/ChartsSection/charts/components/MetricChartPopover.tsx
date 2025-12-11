/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
            <Text size="sm" color="gray">
                {dayjs(datum.x).format('dddd, MMM, D ‘YY')}
            </Text>
            <Text weight="semiBold" size="sm" color="gray">
                {renderDatumMetric(datum)}
            </Text>
        </>
    );
}

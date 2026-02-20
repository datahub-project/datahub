import { TickLabelProps } from '@visx/axis';
import dayjs from 'dayjs';

import { DEFAULT_LENGTH_OF_LEFT_AXIS_LABEL } from '@components/components/BarChart/constants';
import { BarChartProps, Datum } from '@components/components/BarChart/types';
import { abbreviateNumber } from '@components/components/dataviz/utils';

function commonTickLabelProps(fillColor: string): TickLabelProps<Datum> {
    return {
        fontSize: 10,
        fontFamily: 'Mulish',
        fill: fillColor,
    };
}

export function getBarChartDefaults(textColor: string, borderColor: string): Partial<BarChartProps> {
    return {
        xScale: { type: 'band', paddingInner: 0.4, paddingOuter: 0.1 },
        yScale: { type: 'linear', nice: true, round: true },

        leftAxisProps: {
            tickFormat: abbreviateNumber,
            tickLabelProps: {
                ...commonTickLabelProps(textColor),
                textAnchor: 'end',
                width: 50,
            },
            hideAxisLine: true,
            hideTicks: true,
        },
        maxLengthOfLeftAxisLabel: DEFAULT_LENGTH_OF_LEFT_AXIS_LABEL,
        showLeftAxisLine: false,
        bottomAxisProps: {
            tickFormat: (value) => dayjs(value).format('DD MMM'),
            tickLabelProps: {
                ...commonTickLabelProps(textColor),
                textAnchor: 'middle',
                verticalAnchor: 'start',
                width: 20,
            },
            hideAxisLine: true,
            hideTicks: true,
        },
        gridProps: {
            rows: true,
            columns: false,
            stroke: borderColor,
            strokeWidth: 1,
            lineStyle: {},
        },
    };
}

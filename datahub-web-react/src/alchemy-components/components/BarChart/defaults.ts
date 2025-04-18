import { colors } from '@src/alchemy-components/theme';
import { TickLabelProps } from '@visx/axis';
import dayjs from 'dayjs';
import { DEFAULT_LENGTH_OF_LEFT_AXIS_LABEL } from './constants';
import { BarChartProps, Datum } from './types';
import { abbreviateNumber } from '../dataviz/utils';

const commonTickLabelProps: TickLabelProps<Datum> = {
    fontSize: 10,
    fontFamily: 'Mulish',
    fill: colors.gray[1700],
};

export const barChartDefault: Partial<BarChartProps> = {
    xScale: { type: 'band', paddingInner: 0.4, paddingOuter: 0.1 },
    yScale: { type: 'linear', nice: true, round: true },

    leftAxisProps: {
        tickFormat: abbreviateNumber,
        tickLabelProps: {
            ...commonTickLabelProps,
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
            ...commonTickLabelProps,
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
        stroke: '#e0e0e0',
        strokeWidth: 1,
        lineStyle: {},
    },
};

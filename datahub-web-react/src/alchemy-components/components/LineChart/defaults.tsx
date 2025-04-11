import React from 'react';
import { TickLabelProps } from '@visx/axis';
import { colors } from '@src/alchemy-components/theme';
import dayjs from 'dayjs';
import { LinearGradient } from '@visx/gradient';
import { Datum, LineChartProps } from './types';
import { abbreviateNumber } from '../dataviz/utils';
import { roundToEven } from './utils';
import { GLYPH_DROP_SHADOW_FILTER } from './constants';
import { Glyph, TooltipGlyph } from './components';

const commonTickLabelProps: TickLabelProps<Datum> = {
    fontSize: 10,
    fontFamily: 'Mulish',
    fill: colors.gray[1700],
};

export const lineChartDefault: LineChartProps = {
    data: [],
    isEmpty: false,

    xScale: { type: 'time' },
    yScale: { type: 'linear', nice: true, round: true, zero: true },

    lineColor: colors.violet[500],
    areaColor: 'url(#line-gradient)',
    margin: { top: 0, right: 0, bottom: 0, left: 0 },

    leftAxisProps: {
        tickFormat: abbreviateNumber,
        tickLabelProps: {
            ...commonTickLabelProps,
            textAnchor: 'end',
            width: 50,
        },
        computeNumTicks: () => 5,
        hideAxisLine: true,
        hideTicks: true,
    },
    showLeftAxisLine: false,
    bottomAxisProps: {
        tickFormat: (x) => dayjs(x).format('D MMM'),
        tickLabelProps: {
            ...commonTickLabelProps,
            textAnchor: 'middle',
            verticalAnchor: 'start',
        },
        computeNumTicks: (width, _, margin, data) => {
            const widthOfTick = 80;
            const widthOfAxis = width - margin.right - margin.left;
            const maxCountOfTicks = Math.ceil(widthOfAxis / widthOfTick);
            const numOfTicks = roundToEven(maxCountOfTicks / 2);
            return Math.max(Math.min(numOfTicks, data.length - 1), 1);
        },
        hideAxisLine: true,
        hideTicks: true,
    },
    showBottomAxisLine: true,
    gridProps: {
        rows: true,
        columns: false,
        stroke: '#e0e0e0',
        computeNumTicks: () => 5,
        lineStyle: {},
    },

    renderGradients: () => (
        <LinearGradient id="line-gradient" from={colors.violet[200]} to={colors.white} toOpacity={0.6} />
    ),
    toolbarVerticalCrosshairStyle: {
        stroke: colors.white,
        strokeWidth: 2,
        filter: GLYPH_DROP_SHADOW_FILTER,
    },
    renderTooltipGlyph: (props) => <TooltipGlyph {...props} />,
    showGlyphOnSingleDataPoint: true,
    renderGlyphOnSingleDataPoint: Glyph,
};

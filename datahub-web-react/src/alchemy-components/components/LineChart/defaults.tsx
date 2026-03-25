import { TickLabelProps } from '@visx/axis';
import { LinearGradient } from '@visx/gradient';
import dayjs from 'dayjs';
import React from 'react';
import { DefaultTheme, useTheme } from 'styled-components';

import { Glyph, TooltipGlyph } from '@components/components/LineChart/components';
import { GLYPH_DROP_SHADOW_FILTER } from '@components/components/LineChart/constants';
import { Datum, LineChartProps } from '@components/components/LineChart/types';
import { roundToEven } from '@components/components/LineChart/utils';
import { abbreviateNumber } from '@components/components/dataviz/utils';

export function useLineChartDefaults(): LineChartProps {
    const theme = useTheme();
    return getLineChartDefaults(theme);
}

function getCommonTickLabelProps(theme?: DefaultTheme): TickLabelProps<Datum> {
    return {
        fontSize: 10,
        fontFamily: 'Mulish',
        fill: theme?.colors.textSecondary,
    };
}

export function getLineChartDefaults(theme?: DefaultTheme): LineChartProps {
    const commonTickLabelProps = getCommonTickLabelProps(theme);

    return {
        data: [],
        isEmpty: false,

        xScale: { type: 'time' },
        yScale: { type: 'linear', nice: true, round: true, zero: true },
        shouldAdjustYZeroPoint: true,

        lineColor: theme?.colors.iconBrand,
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
            stroke: theme?.colors.border,
            computeNumTicks: () => 5,
            lineStyle: {},
        },

        renderGradients: () => (
            <LinearGradient
                id="line-gradient"
                from={theme?.colors.chartsBrandLow}
                to={theme?.colors.bg}
                toOpacity={0.6}
            />
        ),
        toolbarVerticalCrosshairStyle: {
            stroke: theme?.colors.bg,
            strokeWidth: 2,
            filter: GLYPH_DROP_SHADOW_FILTER,
        },
        renderTooltipGlyph: (props) => <TooltipGlyph {...props} />,
        showGlyphOnSingleDataPoint: true,
        renderGlyphOnSingleDataPoint: Glyph,
    };
}

export const lineChartDefault: LineChartProps = getLineChartDefaults();

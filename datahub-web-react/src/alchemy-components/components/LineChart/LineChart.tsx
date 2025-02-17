import { colors } from '@src/alchemy-components/theme';
import { abbreviateNumber } from '@src/app/dataviz/utils';
import { TickLabelProps } from '@visx/axis';
import { curveMonotoneX } from '@visx/curve';
import { LinearGradient } from '@visx/gradient';
import { ParentSize } from '@visx/responsive';
import { AreaSeries, Axis, AxisScale, Grid, Tooltip, XYChart } from '@visx/xychart';
import dayjs from 'dayjs';
import React, { useState } from 'react';
import { Popover } from '../Popover';
import { ChartWrapper, TooltipGlyph } from './components';
import { LineChartProps } from './types';
import { getMockedProps } from '../BarChart/utils';
import useMergedProps from '../BarChart/hooks/useMergedProps';
import { roundToEven } from './utils';
import { AxisProps, GridProps } from '../BarChart/types';
import { GLYPH_DROP_SHADOW_FILTER } from './constants';
import useAdaptYScaleToZeroValues from '../BarChart/hooks/useAdaptYScaleToZeroValues';
import useMaxDataValue from '../BarChart/hooks/useMaxDataValue';

const commonTickLabelProps: TickLabelProps<any> = {
    fontSize: 10,
    fontFamily: 'Mulish',
    fill: colors.gray[1700],
};

export const lineChartDefault: LineChartProps<any> = {
    data: [],
    isEmpty: false,

    xAccessor: (datum) => datum?.x,
    yAccessor: (datum) => datum?.y,
    xScale: { type: 'time' },
    yScale: { type: 'log', nice: true, round: true, base: 2 },

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
            return Math.min(numOfTicks, data.length - 1);
        },
        hideAxisLine: true,
        hideTicks: true,
    },
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
    renderTooltipGlyph: (props) => <TooltipGlyph x={props.x} y={props.y} />,
};

export function LineChart<DatumType extends object>({
    data,
    isEmpty,

    xAccessor = lineChartDefault.xAccessor,
    yAccessor = lineChartDefault.yAccessor,
    xScale = lineChartDefault.xScale,
    yScale = lineChartDefault.yScale,
    maxYDomainForZeroData,

    lineColor = lineChartDefault.lineColor,
    areaColor = lineChartDefault.areaColor,
    margin,

    leftAxisProps,
    bottomAxisProps,
    gridProps,

    popoverRenderer,
    renderGradients = lineChartDefault.renderGradients,
    toolbarVerticalCrosshairStyle = lineChartDefault.toolbarVerticalCrosshairStyle,
    renderTooltipGlyph = lineChartDefault.renderTooltipGlyph,
}: LineChartProps<DatumType>) {
    const [showGrid, setShowGrid] = useState<boolean>(false);

    // FYI: additional margins to show left and bottom axises
    const internalMargin = {
        top: (margin?.top ?? 0) + 30,
        right: (margin?.right ?? 0) + 30,
        bottom: (margin?.bottom ?? 0) + 35,
        left: (margin?.left ?? 0) + 40,
    };

    const maxDataValue = useMaxDataValue(data, yAccessor);
    const adaptedYScale = useAdaptYScaleToZeroValues(yScale, maxDataValue, maxYDomainForZeroData);

    const accessors = { xAccessor, yAccessor };

    const { computeNumTicks: computeLeftAxisNumTicks, ...mergedLeftAxisProps } = useMergedProps<AxisProps<DatumType>>(
        leftAxisProps,
        lineChartDefault.leftAxisProps,
    );

    const { computeNumTicks: computeBottomAxisNumTicks, ...mergedBottomAxisProps } = useMergedProps<
        AxisProps<DatumType>
    >(bottomAxisProps, lineChartDefault.bottomAxisProps);

    const { computeNumTicks: computeGridNumTicks, ...mergedGridProps } = useMergedProps<GridProps<DatumType>>(
        gridProps,
        lineChartDefault.gridProps,
    );

    // In case of no data we should render empty graph with axises
    // but they don't render at all without any data.
    // To handle this case we will render the same graph with fake data and hide bars
    if (!data.length) {
        return <LineChart {...getMockedProps()} isEmpty />;
    }

    return (
        <ChartWrapper onMouseEnter={() => setShowGrid(true)} onMouseLeave={() => setShowGrid(false)}>
            <ParentSize>
                {({ width, height }) => {
                    return (
                        <XYChart
                            width={width}
                            height={height}
                            xScale={xScale}
                            yScale={adaptedYScale}
                            margin={internalMargin}
                            captureEvents={!isEmpty}
                        >
                            {renderGradients?.()}

                            <Axis
                                orientation="left"
                                numTicks={computeLeftAxisNumTicks?.(width, height, internalMargin, data)}
                                {...mergedLeftAxisProps}
                            />

                            <Axis
                                orientation="bottom"
                                numTicks={computeBottomAxisNumTicks?.(width, height, internalMargin, data)}
                                {...mergedBottomAxisProps}
                            />
                            {/* Left vertical line for y-axis */}
                            <line
                                x1={internalMargin.left}
                                x2={internalMargin.left}
                                y1={0}
                                y2={height - internalMargin.bottom}
                                stroke={mergedGridProps.stroke}
                            />
                            {/* Bottom horizontal line for x-axis */}
                            <line
                                x1={internalMargin.left}
                                x2={width - internalMargin.right}
                                y1={height - internalMargin.bottom}
                                y2={height - internalMargin.bottom}
                                stroke={mergedGridProps.stroke}
                            />

                            {showGrid && (
                                <Grid
                                    numTicks={computeGridNumTicks?.(width, height, internalMargin, data)}
                                    {...mergedGridProps}
                                />
                            )}

                            <AreaSeries<AxisScale, AxisScale, DatumType>
                                dataKey="line-chart-seria-01"
                                data={data}
                                fill={!isEmpty ? areaColor : 'transparent'}
                                curve={curveMonotoneX}
                                lineProps={{ stroke: !isEmpty ? lineColor : 'transparent' }}
                                {...accessors}
                            />

                            <Tooltip<DatumType>
                                snapTooltipToDatumX
                                snapTooltipToDatumY
                                showVerticalCrosshair
                                applyPositionStyle
                                showSeriesGlyphs
                                verticalCrosshairStyle={toolbarVerticalCrosshairStyle}
                                renderGlyph={renderTooltipGlyph}
                                unstyled
                                renderTooltip={({ tooltipData }) => {
                                    return (
                                        tooltipData?.nearestDatum && (
                                            <Popover
                                                open
                                                defaultOpen
                                                placement="topLeft"
                                                key={`${xAccessor(tooltipData.nearestDatum.datum)}`}
                                                content={popoverRenderer?.(tooltipData.nearestDatum.datum)}
                                            />
                                        )
                                    );
                                }}
                            />
                        </XYChart>
                    );
                }}
            </ParentSize>
        </ChartWrapper>
    );
}

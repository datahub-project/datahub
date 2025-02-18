import { abbreviateNumber } from '@src/app/dataviz/utils';
import { curveMonotoneX } from '@visx/curve';
import { ParentSize } from '@visx/responsive';
import { AreaSeries, Axis, AxisScale, GlyphSeries, Grid, Tooltip, XYChart } from '@visx/xychart';
import React, { useMemo, useState } from 'react';
import usePrepareScales from '../BarChart/hooks/usePrepareScales';
import useMergedProps from '../BarChart/hooks/useMergedProps';
import { AxisProps, GridProps } from '../BarChart/types';
import { getMockedProps } from '../BarChart/utils';
import { Popover } from '../Popover';
import { ChartWrapper } from './components';
import { Datum, LineChartProps } from './types';
// FIY: tooltip has a bug when glyph and vertical/horizontal crosshair can be shown behind the graph
// issue: https://github.com/airbnb/visx/issues/1333
// We have this problem when LineChart shown on Drawer
// That can be fixed by adding z-idex
// But there are no ways to do it with StyledComponents as glyph and crosshairs rendered in portals
// https://github.com/styled-components/styled-components/issues/2620
import LeftAxisMarginSetter from '../BarChart/components/LeftAxisMarginSetter';
import './customTooltip.css';
import { lineChartDefault } from './defaults';
import useMinDataValue from '../BarChart/hooks/useMinDataValue';

export function LineChart({
    data,
    isEmpty,

    xScale = lineChartDefault.xScale,
    yScale = lineChartDefault.yScale,
    maxYDomainForZeroData,

    lineColor = lineChartDefault.lineColor,
    areaColor = lineChartDefault.areaColor,
    margin,

    leftAxisProps,
    showLeftAxisLine = lineChartDefault.showLeftAxisLine,
    bottomAxisProps,
    showBottomAxisLine = lineChartDefault.showBottomAxisLine,
    gridProps,

    popoverRenderer,
    renderGradients = lineChartDefault.renderGradients,
    toolbarVerticalCrosshairStyle = lineChartDefault.toolbarVerticalCrosshairStyle,
    renderTooltipGlyph = lineChartDefault.renderTooltipGlyph,
    showGlyphOnSingleDataPoint = lineChartDefault.showGlyphOnSingleDataPoint,
    renderGlyphOnSingleDataPoint = lineChartDefault.renderGlyphOnSingleDataPoint,
}: LineChartProps) {
    const [showGrid, setShowGrid] = useState<boolean>(false);
    const [leftAxisMargin, setLeftAxisMargin] = useState<number>(0);

    // FYI: additional margins to show left and bottom axises
    const internalMargin = useMemo(
        () => ({
            top: (margin?.top ?? 0) + 30,
            right: (margin?.right ?? 0) + 30,
            bottom: (margin?.bottom ?? 0) + 35,
            left: (margin?.left ?? 0) + leftAxisMargin + 6,
        }),
        [leftAxisMargin, margin],
    );

    const xAccessor = (datum: Datum) => datum?.x;
    const yAccessor = (datum: Datum) => datum.y;
    const accessors = { xAccessor, yAccessor };
    const scales = usePrepareScales(data, false, xScale, xAccessor, yScale, yAccessor, maxYDomainForZeroData);

    const { computeNumTicks: computeLeftAxisNumTicks, ...mergedLeftAxisProps } = useMergedProps<AxisProps>(
        leftAxisProps,
        lineChartDefault.leftAxisProps,
    );

    const { computeNumTicks: computeBottomAxisNumTicks, ...mergedBottomAxisProps } = useMergedProps<AxisProps>(
        bottomAxisProps,
        lineChartDefault.bottomAxisProps,
    );

    const { computeNumTicks: computeGridNumTicks, ...mergedGridProps } = useMergedProps<GridProps>(
        gridProps,
        lineChartDefault.gridProps,
    );

    const minDataValue = useMinDataValue(data, yAccessor);

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
                            margin={internalMargin}
                            captureEvents={!isEmpty}
                            {...scales}
                        >
                            {renderGradients?.()}

                            <Axis
                                orientation="left"
                                numTicks={computeLeftAxisNumTicks?.(width, height, internalMargin, data)}
                                {...mergedLeftAxisProps}
                            />
                            <LeftAxisMarginSetter
                                setLeftMargin={setLeftAxisMargin}
                                formatter={leftAxisProps?.tickFormat ?? abbreviateNumber}
                                numOfTicks={computeLeftAxisNumTicks?.(width, height, internalMargin, data)}
                            />

                            <Axis
                                orientation="bottom"
                                numTicks={computeBottomAxisNumTicks?.(width, height, internalMargin, data)}
                                {...mergedBottomAxisProps}
                            />
                            {/* Left vertical line for y-axis */}
                            {showLeftAxisLine && (
                                <line
                                    x1={internalMargin.left}
                                    x2={internalMargin.left}
                                    y1={0}
                                    y2={height - internalMargin.bottom}
                                    stroke={mergedGridProps.stroke}
                                />
                            )}

                            {/* Bottom horizontal line for x-axis */}
                            {showBottomAxisLine && (
                                <line
                                    x1={internalMargin.left}
                                    x2={width - internalMargin.right}
                                    y1={height - internalMargin.bottom}
                                    y2={height - internalMargin.bottom}
                                    stroke={mergedGridProps.stroke}
                                />
                            )}

                            {showGrid && (
                                <Grid
                                    numTicks={computeGridNumTicks?.(width, height, internalMargin, data)}
                                    {...mergedGridProps}
                                />
                            )}

                            <AreaSeries<AxisScale, AxisScale, Datum>
                                dataKey="line-chart-seria-01"
                                data={data}
                                fill={!isEmpty ? areaColor : 'transparent'}
                                curve={curveMonotoneX}
                                lineProps={{ stroke: !isEmpty ? lineColor : 'transparent' }}
                                // adjust baseline to show area correctly with negative values in data
                                y0Accessor={() => Math.min(minDataValue, 0)}
                                {...accessors}
                            />

                            {showGlyphOnSingleDataPoint && data.length === 1 && (
                                <GlyphSeries<AxisScale, AxisScale, Datum>
                                    dataKey="line-chart-seria-01"
                                    data={data}
                                    renderGlyph={renderGlyphOnSingleDataPoint}
                                    {...accessors}
                                />
                            )}

                            <Tooltip<Datum>
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

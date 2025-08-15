import { curveMonotoneX } from '@visx/curve';
import { Group } from '@visx/group';
import { ParentSize } from '@visx/responsive';
import { AreaSeries, Axis, AxisScale, GlyphSeries, Grid, Margin, Tooltip, XYChart } from '@visx/xychart';
import React, { useMemo, useRef, useState } from 'react';

import DynamicMarginSetter from '@components/components/BarChart/components/DynamicMarginSetter';
import useMergedProps from '@components/components/BarChart/hooks/useMergedProps';
import useMinDataValue from '@components/components/BarChart/hooks/useMinDataValue';
import usePrepareScales from '@components/components/BarChart/hooks/usePrepareScales';
import { AxisProps, GridProps } from '@components/components/BarChart/types';
import { getMockedProps } from '@components/components/BarChart/utils';
import { ChartWrapper } from '@components/components/LineChart/components';
// FIY: tooltip has a bug when glyph and vertical/horizontal crosshair can be shown behind the graph
// issue: https://github.com/airbnb/visx/issues/1333
// We have this problem when LineChart shown on Drawer
// That can be fixed by adding z-idex
// But there are no ways to do it with StyledComponents as glyph and crosshairs rendered in portals
// https://github.com/styled-components/styled-components/issues/2620
import '@components/components/LineChart/customTooltip.css';
import { lineChartDefault } from '@components/components/LineChart/defaults';
import { Datum, LineChartProps } from '@components/components/LineChart/types';
import { Popover } from '@components/components/Popover';

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

    const defaultMargin = useMemo(
        () => ({
            top: (margin?.top ?? 0) + 30,
            right: (margin?.right ?? 0) + 0,
            bottom: (margin?.bottom ?? 0) + 35,
            left: (margin?.left ?? 0) + 0,
        }),
        [margin],
    );
    const [dynamicMargin, setDynamicMargin] = useState<Margin>(defaultMargin);

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
    const wrapperRef = useRef<HTMLDivElement>(null);

    // In case of no data we should render empty graph with axises
    // but they don't render at all without any data.
    // To handle this case we will render the same graph with fake data and hide bars
    if (!data.length) {
        return <LineChart {...getMockedProps()} margin={margin} isEmpty />;
    }

    return (
        <ChartWrapper ref={wrapperRef} onMouseEnter={() => setShowGrid(true)} onMouseLeave={() => setShowGrid(false)}>
            <ParentSize>
                {({ width, height }) => {
                    return (
                        <XYChart
                            width={width}
                            height={height}
                            margin={dynamicMargin}
                            captureEvents={!isEmpty}
                            {...scales}
                        >
                            {renderGradients?.()}

                            <DynamicMarginSetter
                                setMargin={setDynamicMargin}
                                wrapperRef={wrapperRef}
                                minimalMargin={defaultMargin}
                            />

                            <Axis
                                orientation="left"
                                numTicks={computeLeftAxisNumTicks?.(width, height, dynamicMargin, data)}
                                axisClassName="left-axis"
                                {...mergedLeftAxisProps}
                            />

                            <Axis
                                orientation="bottom"
                                numTicks={computeBottomAxisNumTicks?.(width, height, dynamicMargin, data)}
                                tickClassName="bottom-axis-tick"
                                {...mergedBottomAxisProps}
                            />

                            <Group className="content-group">
                                {/* Left vertical line for y-axis */}
                                {showLeftAxisLine && (
                                    <line
                                        x1={dynamicMargin.left}
                                        x2={dynamicMargin.left}
                                        y1={0}
                                        y2={height - dynamicMargin.bottom}
                                        stroke={mergedGridProps.stroke}
                                    />
                                )}

                                {/* Bottom horizontal line for x-axis */}
                                {showBottomAxisLine && (
                                    <line
                                        x1={dynamicMargin.left}
                                        x2={width - dynamicMargin.right}
                                        y1={height - dynamicMargin.bottom}
                                        y2={height - dynamicMargin.bottom}
                                        stroke={mergedGridProps.stroke}
                                    />
                                )}

                                {showGrid && (
                                    <Grid
                                        numTicks={computeGridNumTicks?.(width, height, dynamicMargin, data)}
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
                            </Group>

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
